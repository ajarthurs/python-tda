# python-tda
TD Ameritrade API Python Wrapper (Unofficial)

This is a working but incomplete prototype written specifically for AutoAssets (a separate project) to enable access to a TD Ameritrade brokerage account. Incorporating this project into production is not recommended as this project may later be deprecated in favor of an official release from TD Ameritrade. Also since this project is in alpha, there may be dependency-breaking changes without notice.

## Features:
* OAuth 2.0 Authentication with a TD Ameritrade account
* Endpoints:
  * GET Quotes, Historical Data, Option Chains
  * POST Order
* WebSockets-based Streaming:
  * Quotes subscription
  * Historical Data subscription
  * Option Chains subscription
  * Account subscription
    * Order updates such as fill status and price

## Installation
TODO: Create `setup.py`.

## Getting Started [^1]:
1. Register for a new TD Ameritrade Developer account.
2. Log into TD Ameritrade's Developer Portal and navigate to `My Apps` create a new App.
3. Given the new App's `Consumer Key` and `Redirect URL`, build the URL needed to authenticate the brokerage account:
```python
import tda.api
print(tda.api.build_oauth_url(
    app_key=<Consumer Key>,
    redirect_url=<Redirect URL>,
))
```
4. Navigate to the provided authentication URL, enter credentials for the brokerage account, click `Allow` to grant the new App's access to the brokerage account, and copy the value of `code` in the URL's query string; `code` is the authorization code. Alternatively, setup a web server, reachable at the redirect URL, to capture the authorization code.
5. Cache the authorization code [^2] to enable `tda` endpoint and streaming features.
```python
import tda.api
oauth_token_dict = tda.api.renew_oauth_tokens(
    app_key=<Consumer Key>,
    redirect_url=<Redirect URL>,
    code_encoded=<Authorization Code>,
)
tda.api.cache_oauth_tokens(oauth_token_dict)
```

## Usage:
* Endpoints:
```python
import tda.api
print(tda.api.get_quotes(['SPY', '$SPX.X']))
print(tda.api.get_history('SPY',
    period_type=tda.PeriodType.YEAR,
    period=1,
    frequency_type=tda.FrequencyType.DAILY,
    frequency=1
))
print(tda.api.get_option_chains('SPY'))

# Example: Buy 100 shares of SPY at market value during the cash market.
tda.api.post_order(account_id=<Brokerage Account ID>, new_order_specs={
        'orderType': tda.api.OrderType.MARKET.value,
        'session': tda.api.OrderSession.NORMAL.value,
        'duration': tda.api.OrderDuration.DAY.value,
        'orderStrategyType': tda.api.OrderStrategyType.SINGLE.value,
        'orderLegCollection': [
          {
            'instruction': tda.api.OrderDirection.BUY.value,
            'quantity': 100,
            'instrument': {
                'symbol': 'SPY',
                'assetType': tda.InstrumentType.EQUITY.value,
            },
          },
        ],
})

# Example: Sell an SPX put spread at market value during the cash market.
tda.api.post_order(account_id=<Brokerage Account ID>, new_order_specs={
        'orderType': tda.api.OrderType.MARKET.value,
        'session': tda.api.OrderSession.NORMAL.value,
        'duration': tda.api.OrderDuration.DAY.value,
        'orderStrategyType': tda.api.OrderStrategyType.SINGLE.value,
        'orderLegCollection': [
          {
            'instruction': tda.api.OrderDirection.SELL_TO_OPEN.value,
            'quantity': 1,
            'instrument': {
                'symbol': 'SPXW_071822P3760',
                'assetType': tda.InstrumentType.OPTION.value,
            },
          },
          {
            'instruction': tda.api.OrderDirection.BUY_TO_OPEN.value,
            'quantity': 1,
            'instrument': {
                'symbol': 'SPXW_071822P3500',
                'assetType': tda.InstrumentType.OPTION.value,
            },
          },

        ],
})
```

## TD Ameritrade Developer Portal:
https://developer.tdameritrade.com/

[^1]: https://developer.tdameritrade.com/content/getting-started
[^2]: The authorization code is valid for a limited amount of time and must be renewed afterwards (see https://developer.tdameritrade.com/content/simple-auth-local-apps).
