# Institutional Ownership and Crowding Factor Spec

## 1. Objective

Design one or more quarterly institutional ownership and crowding factors from Form 13F holdings data. The factors should be usable in the same style as the existing short interest factor pipeline:

``` text
universe
  -> institutional_ownership_raw
  -> institutional_holdings_normalized
  -> institutional_ownership_factor
```

The first production target should be a robust stock-level cross-sectional signal, not a manager-picking product. The preferred output is one row per `(ticker, period)` with standardized factor exposures.

The central design principle is point-in-time conservatism: 13F data should be treated as slow, public, long-only institutional positioning data. It is useful for ownership, sponsorship, breadth, and crowding risk, but it is not real-time order flow.

This spec describes the full design space for institutional ownership and crowding factors using 13F filings. For the MVP, the proposed implementation should cover the raw ingestion layer, normalized holdings layer, market-cap denominator join, and two robust factor outputs: `io_level` and `io_breadth`. The change, flow, and concentration factors are included as extensions once security mapping, point-in-time float data, split adjustment, and amendment handling are confirmed.

MVP denominator decision: use market cap as the ownership-level denominator to support a real historical panel. Massive's latest free-float endpoint does not provide point-in-time historical float, and using latest float for past quarters would create a misleading historical series. Market cap is an imperfect denominator because it includes non-tradable/strategic holdings, but for the MVP we prioritize producing a real three-year history. TODO: switch the denominator provider to point-in-time float market cap when historical float data becomes available.

## 2. Background and Constraints

Form 13F is filed by institutional investment managers that exercise investment discretion over at least USD 100 million in Section 13(f) securities. The SEC states that reportable securities primarily include U.S. exchange-traded stocks, closed-end funds, ETFs, and certain convertible debt, options, and warrants. Managers report issuer name, security class, number of shares, and fair market value as of quarter end.

Important constraints:

-   Form 13F is quarterly and delayed. Filings are due within 45 calendar days after quarter end, so a factor must not be available before the filing is public.
-   Form 13F is long-disclosure data. It does not disclose short positions, cash, many non-U.S. holdings, or full portfolio economics.
-   13F may include ETFs, options, warrants, convertible debt, and other non-common-equity instruments. A stock factor should filter these carefully.
-   Amended filings can correct, supplement, or add omitted holdings, including holdings previously under confidential treatment.
-   The API returns holding-level records keyed by CIK, filing, period, CUSIP/issuer, and value/share fields. It is not directly ticker-level factor data.

## 3. Recommended Factor Set

The full design supports several related factor families, but the MVP should start with the most robust and auditable signals: `io_level` and `io_breadth`. `io_change`, `io_dbreadth`, `io_crowding_flow`, and `io_concentration` can be added after the normalized holdings layer is stable.

### 3.1 IO_LEVEL: Institutional Ownership Level

MVP priority: required.

Definition:

``` text
io_value_i,t = sum_f market_value_f,i,t
market_cap_i,t = point-in-time market capitalization at 13F period end
io_level_raw_i,t = io_value_i,t / market_cap_i,t
io_level_i,t = zscore_by_period(winsorize(io_level_raw_i,t))
```

Preferred future share-based equivalent when reported shares and point-in-time float shares are reliable:

``` text
io_pct_i,t = total_13f_common_shares_i,t / float_shares_i,t
io_pct_factor_i,t = zscore_by_period(winsorize(io_pct_i,t))
```

Interpretation:

``` text
How institutionally owned or institutionally visible this stock is,
relative to the universe in the same quarter.
```

Use case:

-   Risk model descriptor for institutional sponsorship.
-   Control variable for liquidity/common ownership risk.
-   Less directly alpha-like than flow/crowding, but stable and explainable.

Default sign convention:

``` text
higher = more institutionally owned
```

Implementation notes:

-   Aggregate by `(period, security_id)` after mapping CUSIP to ticker/security id.
-   MVP uses `market_value / market_cap` to support a real historical panel.
-   This should not be labeled as a strict ownership percentage because market cap is not float-adjusted.
-   The provider should expose a generic denominator contract so the market-cap denominator can be replaced later without changing factor logic.
-   TODO: replace market cap with point-in-time float market cap when historical float data is available.

``` text
future_io_pct_float_i,t = total_13f_shares_i,t / float_shares_i,t
```

Until point-in-time float shares or float market capitalization are available, do not call the MVP level signal "ownership percentage"; call it "13F reported ownership value scaled by market cap".

### 3.2 IO_CHANGE: Change in Institutional Ownership

MVP priority: phase 2, after at least two clean quarterly normalized holdings snapshots exist.

Definition:

``` text
dio_pct_i,t = io_pct_i,t - io_pct_i,t-1
dio_factor_i,t = zscore_by_period(winsorize(dio_pct_i,t))
```

Fallback when share-based deltas are unavailable:

``` text
dio_value_scaled_i,t =
  (total_13f_market_value_i,t - total_13f_market_value_i,t-1) / market_cap_i,t-1
```

Interpretation:

``` text
Whether institutions increased or reduced aggregate ownership since the prior 13F period.
```

Use case:

-   More marginal-demand-like than ownership level.
-   Useful as both an institutional sponsorship signal and a crowding risk input.

Default sign convention:

``` text
higher = aggregate institutional ownership increased more
```

### 3.3 IO_BREADTH: Breadth of Institutional Ownership

MVP priority: required.

Definition:

``` text
holder_count_i,t = count_distinct(filer_cik holding stock i at period t)
breadth_pct_i,t = holder_count_i,t / active_13f_filer_count_t
io_breadth_raw_i,t = log1p(holder_count_i,t)
io_breadth_i,t = zscore_by_period(winsorize(io_breadth_raw_i,t))
```

Optional active-holder variant:

``` text
active_holder_count_i,t =
  count_distinct(filer_cik where position weight in manager portfolio >= min_weight)
```

Interpretation:

``` text
How widely held the stock is across reporting institutions.
```

Use case:

-   More robust than dollar value when very large managers dominate value.
-   Useful as a proxy for investor base breadth and common institutional attention.
-   Academic motivation: breadth is a common institutional ownership construct because it captures how widely ownership is distributed across investors, not only how many dollars are held.

Default sign convention:

``` text
higher = held by more institutions
```

Implementation notes:

-   Count each `(filer_cik, security_id, period)` once.
-   If the same manager has multiple filings for the same period due to amendment logic, deduplicate before counting.
-   Do not use raw row count, because a manager can report multiple lines for related classes or option positions.

### 3.4 IO_DBREADTH: Change in Breadth

MVP priority: phase 2, after at least two clean quarterly normalized holdings snapshots exist.

Definition:

``` text
dbreadth_i,t = breadth_pct_i,t - breadth_pct_i,t-1
dbreadth_factor_i,t = zscore_by_period(winsorize(dbreadth_i,t))
```

Interpretation:

``` text
Whether more institutions began holding the stock, or existing institutions exited, since the prior quarter.
```

Use case:

-   Often cleaner than share-weighted flow because it is less dominated by mega-managers.
-   Useful when split-adjusted shares or precise portfolio values are noisy.

Default sign convention:

``` text
higher = ownership base broadened
```

### 3.5 IO_CROWDING_FLOW: Institutional Crowding / Flow Imbalance

MVP priority: future extension.

Definition:

For each manager and stock, compute quarter-over-quarter change in reported shares or value:

``` text
delta_shares_f,i,t = shares_f,i,t - split_adjusted_shares_f,i,t-1
buy_flag_f,i,t  = 1 if delta_shares_f,i,t > min_change
sell_flag_f,i,t = 1 if delta_shares_f,i,t < -min_change
```

Then aggregate to stock-level institutional imbalance:

``` text
buyer_count_i,t = sum_f buy_flag_f,i,t
seller_count_i,t = sum_f sell_flag_f,i,t
active_count_i,t = buyer_count_i,t + seller_count_i,t

imbalance_i,t =
  (buyer_count_i,t - seller_count_i,t) / max(active_count_i,t, 1)
```

Dollar-weighted alternative:

``` text
dollar_imbalance_i,t =
  sum_f delta_value_f,i,t / sum_f abs(delta_value_f,i,t)
```

Final factor:

``` text
io_crowding_flow_i,t = zscore_by_period(winsorize(imbalance_i,t))
```

Interpretation:

``` text
Whether institutions are broadly moving into or out of the same stock.
```

Use case:

-   Crowding/herding risk indicator.
-   Potential contrarian input after public disclosure, because 13F flow is stale and may represent already-consumed demand.

Default sign convention:

``` text
higher = more institutions recently accumulated the stock
```

Research hypothesis:

-   As a risk descriptor: high absolute crowding means positioning is one-sided and the stock may be vulnerable to unwind risk.
-   As an alpha signal: the direct sign should not be assumed. Test both momentum continuation and contrarian versions:

``` text
io_flow_momentum = +io_crowding_flow
io_flow_contrarian = -io_crowding_flow
io_crowding_pressure = abs(io_crowding_flow)
```

Recommended MVP:

Ship `io_crowding_flow` and `io_crowding_pressure`; do not hard-code an alpha sign until backtested.

### 3.6 IO_CONCENTRATION: Owner Concentration / Fragility

MVP priority: future extension.

Definition:

``` text
owner_share_f,i,t = shares_f,i,t / sum_f shares_f,i,t
hhi_owners_i,t = sum_f owner_share_f,i,t^2
top5_owner_share_i,t = sum_largest_5(owner_share_f,i,t)
top10_owner_share_i,t = sum_largest_10(owner_share_f,i,t)
```

Final factor:

``` text
io_concentration_i,t = zscore_by_period(winsorize(hhi_owners_i,t))
```

Interpretation:

``` text
Whether the reported institutional ownership base is concentrated in a few large holders.
```

Use case:

-   Crowding fragility risk: high institutional ownership plus high concentration can indicate unwind vulnerability.
-   Risk overlay rather than a standalone alpha signal.

Default sign convention:

``` text
higher = ownership is more concentrated
```

## 4. Data Model

### 4.1 Raw Asset

Asset name:

``` text
institutional_ownership_raw
```

Grain:

``` text
one row per Massive 13F holding record
```

Suggested columns:

``` text
filer_cik
accession_number
filing_date
period
form_type
cusip
issuer_name
title_of_class
market_value
shares_or_principal_amount
shares_or_principal_type
put_call
investment_discretion
voting_authority_sole
voting_authority_shared
voting_authority_none
filing_url
```

Raw fetch modes:

-   Backfill: request historical filings by `filing_date` or period windows.
-   Incremental: request filings with `filing_date >= last_successful_filing_date`.

### 4.2 Normalized Holdings Asset

Asset name:

``` text
institutional_holdings_normalized
```

Grain:

``` text
one row per (filer_cik, period, security_id)
```

Purpose:

-   Filter to common-equity-like holdings.
-   Resolve amendments.
-   Map CUSIP/FIGI/issuer to internal security id and ticker.
-   Join market-cap denominator for MVP historical coverage.
-   Deduplicate manager/security/period records.
-   Convert numeric columns to consistent units.

Suggested columns:

``` text
filer_cik
period
filing_date
security_id
ticker
cusip
issuer_name
market_value
shares
denominator_value
denominator_type
market_cap
source_accession_number
is_amended
is_option
is_common_equity
```

TODO: add point-in-time `float_shares` and `float_market_cap` columns when a historical float source is available.

### 4.3 Factor Asset

Asset names:

``` text
institutional_ownership_factor_full
institutional_ownership_factor_incremental
```

Output grain:

``` text
one row per (ticker, period)
```

Suggested columns:

``` text
ticker
period
asof_date
io_level
io_pct
io_change
io_breadth
io_dbreadth
io_crowding_flow
io_crowding_pressure
io_concentration
holder_count
active_filer_count
total_13f_market_value
total_13f_shares
denominator_value
denominator_type
market_cap
buyer_count
seller_count
active_count
hhi_owners
top5_owner_share
top10_owner_share
```

`asof_date` must be the date when the signal becomes usable, not quarter end:

``` text
point-in-time incremental mode:
  asof_date = actual filing_date, shifted to the next trading day

simplified quarterly research mode:
  asof_date = period + 45 calendar days, shifted to the next trading day
```

The first mode is more accurate and should be preferred for production backtests. The second mode is more conservative and easier to implement for a first research panel.

## 5. Point-in-Time Rules

The most important implementation rule is that `period` is not the trading date.

Example:

``` text
period = 2024-09-30
filing_date = 2024-11-14
```

The holdings describe quarter-end 2024-09-30 positions, but the market could not use them until the filing appeared. For backtesting:

``` text
effective_date = next_trading_day(filing_date)
```

Recommended conventions:

-   Use actual `filing_date` for each manager when constructing incremental public information.
-   For a simpler quarterly research panel, use a conservative release date of `period + 45 calendar days`, then shift to next trading day.
-   Never use 13F data in returns before its public filing date.
-   Store both `period` and `asof_date` in factor outputs.
-   Maintain two datasets when possible:

``` text
as_reported_first_available = what was public at each historical date
latest_corrected = latest known corrected/amended truth
```

Use `as_reported_first_available` for tradable backtests. Use `latest_corrected` for descriptive analysis and data QA.

## 6. Filtering and Cleaning Rules

### 6.1 Instrument Filters

For the initial equity factor, include:

``` text
put_call is null
shares_or_principal_type == "SH"
title_of_class indicates common or ordinary equity where possible
mapped ticker is in target universe
```

Exclude or separately tag:

``` text
PUT / CALL option records
ETFs
closed-end funds
convertible debt
warrants
unmapped CUSIPs
ambiguous issuer mappings
```

Rationale:

The stock-level factor should describe direct institutional ownership of common equity, not option exposure or fund holdings.

### 6.2 Amendment Handling

Amendment policy:

-   Treat `13F-HR/A` as replacing or supplementing the original filing for the same `(filer_cik, period)`.
-   If the amendment is a full restatement, use the latest accepted filing.
-   If the amendment only adds omitted holdings, combine it with the original after de-duplicating by security.
-   Store `source_accession_number` and `is_amended`.

Open implementation question:

Massive may already normalize amendments. Confirm exact behavior before coding replacement logic.

### 6.3 CUSIP to Ticker Mapping

Preferred key:

``` text
security_id
```

Do not rely on issuer name alone. Use a point-in-time security master when available. At minimum, map by CUSIP and keep mapping validity dates.

Risks:

-   CUSIPs can change after corporate actions.
-   Multiple share classes can map to different tickers.
-   Issuer names are not stable enough for production joins.

### 6.4 Share Adjustments

For the MVP ownership level based on reported `market_value`, the denominator is point-in-time `market_cap` so the factor can support a real historical panel. This is a deliberate compromise and should remain documented. TODO: replace this denominator with point-in-time `float_market_cap` when historical float data is available.

For flow based on `delta_shares`, split adjustments are required:

``` text
split_adjusted_shares_f,i,t-1 =
  prior_shares_f,i,t-1 * split_factor_between(t-1, t)
```

If split data is unavailable in the MVP, prefer value-based flow or count-based entry/exit flow, and clearly label it as approximate.

### 6.5 Manager Universe Drift

The number and composition of 13F filers changes through time. Breadth and buyer/seller counts should therefore be normalized:

``` text
breadth_pct_i,t = holder_count_i,t / active_13f_filer_count_t
buyer_pct_i,t = buyer_count_i,t / active_13f_filer_count_t
seller_pct_i,t = seller_count_i,t / active_13f_filer_count_t
```

Keep raw counts as diagnostics, but prefer normalized versions for cross-time comparisons.

## 7. Factor Construction Details

### 7.1 Cross-Section

All final factor values are computed cross-sectionally by `period` within the selected universe:

``` text
factor_i,t = zscore(winsorize(raw_metric_i,t))
```

Default winsorization:

``` text
lower = 1st percentile
upper = 99th percentile
```

Default z-score:

``` text
z_i = (x_i - mean(x)) / std(x)
```

If `std == 0`, set factor to `0.0`.

### 7.2 Missing Holdings

If a stock is in the universe but has no 13F holdings for a period:

``` text
io_value = 0
holder_count = 0
flow metrics = 0 or null depending on prior availability
```

Recommended:

-   For `io_level`, use zero reported 13F value over the stock's point-in-time market cap in the MVP.
-   For `io_breadth`, use zero holders before `log1p`.
-   For flow, require both current and previous period to compute manager-level deltas. If prior data is missing, set flow metric to null and exclude from flow z-score.

### 7.3 Sector or Size Neutralization

Initial MVP should produce raw cross-sectional z-scores only.

Second pass should consider optional neutralized variants:

``` text
io_level_size_neutral
io_pct_size_neutral
io_change_size_neutral
io_breadth_size_neutral
io_dbreadth_size_neutral
io_flow_sector_neutral
```

Rationale:

Institutional ownership is mechanically related to float market cap, total market cap, index membership, liquidity, and sector. Raw values are useful as descriptors, but alpha research should control for size and sector.

## 8. Validation and Asset Checks

Recommended Tier 1 inline failures:

-   Raw fetch returns zero rows for a requested production window.
-   Required columns missing from raw API response.
-   More than 50% of raw rows have null `period`, `filer_cik`, or `cusip`.

Recommended Tier 2 blocking checks:

-   Duplicate normalized key count for `(filer_cik, period, security_id)` after amendment handling.
-   Mapping coverage below 80% by raw market value.
-   Factor output has duplicate `(ticker, period)`.
-   Factor output missing required columns.
-   Factor `mean` or `std` by period deviates materially from 0/1 after z-scoring.

Recommended Tier 3 warning checks:

-   Mapping coverage below 95% by raw market value.
-   High unmapped CUSIP count.
-   High ETF/option exclusion share.
-   Large quarter-over-quarter jump in holder count or aggregate market value.
-   Very low active count for flow factor.
-   High concentration factor computed from fewer than a minimum number of holders.

Useful metadata to log:

``` text
rows_raw
rows_normalized
period_count
filer_count
mapped_market_value_ratio
excluded_option_rows
excluded_etf_rows
amended_filing_count
duplicate_count
```

## 9. Backtest Hygiene

Minimum acceptable backtest rules:

-   Signal date must be after public filing date, not quarter end.
-   Use point-in-time universe membership when available.
-   Use point-in-time CUSIP-to-ticker mapping when available.
-   Use point-in-time market cap for the MVP denominator to support a real historical panel.
-   Document that market cap is not float-adjusted and can understate ownership intensity for low-float companies.
-   TODO: switch to point-in-time float shares or float market capitalization when historical float data is available.
-   Do not include amended/confidential holdings before they became public.
-   Test with realistic quarterly rebalance cadence.
-   Compare raw, size-neutral, and sector-neutral versions.
-   Report turnover, capacity, liquidity exposure, and correlation with size, value, momentum, short interest, and beta.

Recommended evaluation horizons:

``` text
1 month after signal effective date
3 months after signal effective date
next 13F cycle
```

Recommended portfolios:

``` text
top-bottom quintile spread
decile long-short where coverage permits
rank IC by period
sector-neutral rank IC
```

Recommended diagnostics:

``` text
correlation with float market cap and total market cap
correlation with dollar volume
correlation with index membership
correlation with short interest factor
turnover by quarter
coverage by sector
coverage by market-cap bucket
```

## 10. Proposed Dagster Asset Graph

``` text
sp500_universe
security_master
      |
      v
institutional_ownership_raw
      |
      v
institutional_holdings_normalized
      |
      v
institutional_ownership_factor_full
institutional_ownership_factor_incremental
```

Resource:

``` text
Massive13FProvider
```

Provider interface:

``` python
class InstitutionalHoldingsProvider(Protocol):
    def fetch_13f(
        self,
        filing_date_gte: str | None = None,
        filing_date_lte: str | None = None,
        filer_cik: str | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        ...
```

## 11. MVP Recommendation

Build in this order:

1.  `institutional_ownership_raw`
2.  `institutional_holdings_normalized`
3.  Join point-in-time market cap denominator
4.  `io_level`
5.  `io_breadth`

MVP output:

``` text
ticker
period
asof_date
io_level
io_breadth
holder_count
active_filer_count
total_13f_market_value
denominator_value
denominator_type
market_cap
```

MVP limitation: the denominator is market cap, not point-in-time float market cap. This supports a real three-year history but can understate ownership intensity for low-float companies. TODO: switch the denominator provider to point-in-time float market cap when historical float data becomes available.

Phase 2:

1.  `io_change`
2.  `io_dbreadth`
3.  `io_concentration`

Future extensions:

1.  `io_crowding_flow` using count-based buyer/seller imbalance
2.  Split-adjusted share/value flow after float data and security master support exist
3.  Size, sector, and liquidity neutralized variants
