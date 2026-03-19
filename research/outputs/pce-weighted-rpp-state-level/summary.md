# Weighted RPP State-Level Research

Most recent year analyzed: **2024**.

## Key Takeaways

- Excluding California alone lowers `Housing rents` the most: -7.7459 RPP points (-7.1758%).
- Excluding California, New York, New Jersey, Illinois, and Connecticut lowers `Housing rents` the most: -11.2659 RPP points (-10.4368%).
- The national baseline includes DC because the serving view contains 51 geographies (50 states plus DC).
- Prompt category `housing` is reported here as `Housing rents`, matching the serving view contract.

## Question 1: Latest-Year National vs Exclusion Scenarios (2024)

| Category | Scenario | RPP | Share of national PCE | Difference vs national | % change vs national |
| --- | --- | --- | --- | --- | --- |
| All items | National | 100.4801 | 100.0000% | +0.0000 | +0.0000% |
| All items | Without CA | 98.8957 | 86.6007% | -1.5844 | -1.5768% |
| All items | Without CA, NY, NJ, IL, CT | 97.4870 | 71.7093% | -2.9931 | -2.9788% |
| Housing rents | National | 107.9443 | 100.0000% | +0.0000 | +0.0000% |
| Housing rents | Without CA | 100.1984 | 85.6948% | -7.7459 | -7.1758% |
| Housing rents | Without CA, NY, NJ, IL, CT | 96.6784 | 71.4497% | -11.2659 | -10.4368% |
| Goods | National | 99.7595 | 100.0000% | +0.0000 | +0.0000% |
| Goods | Without CA | 98.8969 | 88.0209% | -0.8626 | -0.8647% |
| Goods | Without CA, NY, NJ, IL, CT | 97.6866 | 74.2917% | -2.0730 | -2.0780% |
| Utilities | National | 104.4351 | 100.0000% | +0.0000 | +0.0000% |
| Utilities | Without CA | 97.0457 | 88.0533% | -7.3894 | -7.0756% |
| Utilities | Without CA, NY, NJ, IL, CT | 93.3108 | 74.7310% | -11.1243 | -10.6519% |
| Other services | National | 100.2276 | 100.0000% | +0.0000 | +0.0000% |
| Other services | Without CA | 99.8413 | 85.9504% | -0.3863 | -0.3855% |
| Other services | Without CA, NY, NJ, IL, CT | 99.1628 | 70.0548% | -1.0647 | -1.0623% |

## Question 2: Five-Year Change by State and Category

Largest five-year increase by state:

- `CA`: `Utilities` rose 25.399 RPP points (19.025%) from 2020 to 2024.
- `IL`: `Goods` rose 2.520 RPP points (2.487%) from 2020 to 2024.
- `NJ`: `Goods` rose 1.564 RPP points (1.482%) from 2020 to 2024.
- `NY`: `Goods` rose 1.555 RPP points (1.471%) from 2020 to 2024.
- `CT`: `Other services` rose 0.218 RPP points (0.213%) from 2020 to 2024.

### California

| Category | RPP 2020 | RPP 2024 | 5-year change | 5-year % change |
| --- | --- | --- | --- | --- |
| All items | 111.944 | 110.720 | -1.224 | -1.093% |
| Goods | 106.661 | 106.098 | -0.563 | -0.528% |
| Housing rents | 166.329 | 154.346 | -11.983 | -7.204% |
| Other services | 101.934 | 102.591 | +0.657 | +0.645% |
| Utilities | 133.500 | 158.899 | +25.399 | +19.025% |

### New York

| Category | RPP 2020 | RPP 2024 | 5-year change | 5-year % change |
| --- | --- | --- | --- | --- |
| All items | 110.130 | 107.921 | -2.209 | -2.006% |
| Goods | 105.699 | 107.254 | +1.555 | +1.471% |
| Housing rents | 132.330 | 122.168 | -10.162 | -7.679% |
| Other services | 105.872 | 104.067 | -1.805 | -1.705% |
| Utilities | 138.698 | 134.370 | -4.328 | -3.120% |

### New Jersey

| Category | RPP 2020 | RPP 2024 | 5-year change | 5-year % change |
| --- | --- | --- | --- | --- |
| All items | 110.722 | 108.805 | -1.917 | -1.731% |
| Goods | 105.502 | 107.066 | +1.564 | +1.482% |
| Housing rents | 139.496 | 134.292 | -5.204 | -3.731% |
| Other services | 106.010 | 103.529 | -2.481 | -2.340% |
| Utilities | 120.726 | 114.187 | -6.539 | -5.416% |

### Illinois

| Category | RPP 2020 | RPP 2024 | 5-year change | 5-year % change |
| --- | --- | --- | --- | --- |
| All items | 100.315 | 99.958 | -0.357 | -0.356% |
| Goods | 101.312 | 103.832 | +2.520 | +2.487% |
| Housing rents | 97.642 | 93.944 | -3.698 | -3.787% |
| Other services | 101.418 | 100.156 | -1.262 | -1.244% |
| Utilities | 84.741 | 84.952 | +0.211 | +0.249% |

### Connecticut

| Category | RPP 2020 | RPP 2024 | 5-year change | 5-year % change |
| --- | --- | --- | --- | --- |
| All items | 105.119 | 103.610 | -1.509 | -1.436% |
| Goods | 99.011 | 97.330 | -1.681 | -1.698% |
| Housing rents | 123.162 | 117.048 | -6.114 | -4.964% |
| Other services | 102.440 | 102.658 | +0.218 | +0.213% |
| Utilities | 159.900 | 146.495 | -13.405 | -8.383% |

## Method Notes

- Source table: `serving.v_state_rpp_pce_weighted_annual`.
- Subset scenarios are renormalized with `SUM(weighted_rpp) / SUM(pce_share)` over included states.
- `Other services` is derived in the serving view rather than mapped from a single raw BEA line.
- `Housing rents` uses the view's documented housing proxy mapping.
