## Purpose

Records which securities belong to the tracked market indexes, and since when, so consumers can list current and past members.

## ADDED Requirements

### Requirement: Tracked indexes and their sources
The sync SHALL maintain the memberships of the Dow Jones Industrial, Transportation and Utility Averages, the NASDAQ-100 and the S&P 500. The Dow Jones Industrial Average SHALL be read from Wikipedia's `List_of_Dow_Jones_Industrial_Average_companies`.

#### Scenario: All indexes load
- **WHEN** the sources are reachable
- **THEN** the run records 30, 20, 15, about 100 and about 500 members for the five indexes respectively

### Requirement: Memberships have periods
A membership SHALL record when the security joined and, once it leaves, when it left. A security that leaves an index SHALL keep its past membership.

#### Scenario: A component is replaced
- **WHEN** an index's source no longer lists security A and newly lists security B
- **THEN** A's membership ends on the sync date and B's starts on it

### Requirement: A failing source affects only its index
When one index's source fails, is throttled after retries, or no longer has the expected table, that index's memberships SHALL stay unchanged, the run SHALL record the error naming the source, and the other indexes SHALL still sync.

#### Scenario: Wikipedia changes a page
- **WHEN** the Dow Jones Transportation page has no table with a ticker column
- **THEN** its memberships are unchanged, the run records the error with the page URL, and the S&P 500 still syncs
