# EPP Analysis Scripts

Analysis scripts to measure behaviours around EPP selections.

## Request Trace

### Run

#### Pre-Reqs
* Ensure `GRAFANA_URL` and `GRAFANA_SERVICE_ACCOUNT_TOKEN` env variables are set
* Locate a request to trace - these can be picked up from the `x_request_id` parameter in EPP log lines, e.g using a query such as:
  ```promql
  {app="glm-5-2-precise-epp", namespace="vortex"} | json | msg="Selecting endpoints from candidates sorted by max score"
  ```


#### How to run

* Run with `uv` using:
  ```bash
  uv run epp_request_trace.py --request-id {request-id} --since-hours 2
  ```

Results will look like:

```
Looking up {request ID}...
Fetching GPU utilization...

==========================================================================
Request  : {request ID}
Time     : 2026-08-26T21:59:21Z
Winner   : ...mvl7p   score=0.9967988   GPU=0/0/0%
Scored   : 7 candidates   (7 passed to picker)   (50 filtered out)
Prefix   : glm-5-2-vllm-public-8bff7648c-
==========================================================================
Suffix       Score  GPU min/avg/max  Status
---------------------------------------------------
{chosen-pod}    0.9967988         0/0/0%  <-- SELECTED

{candidate-pod-1}    0.9967988   100/100/100%  candidate
{candidate-pod-2}    0.9964681    98/100/100%  candidate
{candidate-pod-3}    0.9937294     98/99/100%  candidate
{candidate-pod-4}    0.9926815     36/91/100%  candidate
{candidate-pod-5}    0.9967988       0/66/99%  candidate
{candidate-pod-6}    0.9962103      0/62/100%  candidate

{filtered-pod-1}   (filtered)   100/100/100%  filtered out
{filtered-pod-2}   (filtered)   100/100/100%  filtered out
{filtered-pod-3}   (filtered)   100/100/100%  filtered out
{filtered-pod-4}   (filtered)   100/100/100%  filtered out
...
{filtered-pod-last}   (filtered)         0/0/0%  filtered out
```
