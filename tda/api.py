"""
TDA API utility library.
"""

import datetime
import json
import logging
import pickle
import pytz
import requests
import tda
import urllib.parse
from enum import Enum, auto

logger = logging.getLogger(__name__)

class FrequencyType(Enum):
    """
    Aggregation period.
    """
    MINUTELY = 'minute'
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: FrequencyType

class OAuthGrantType(Enum):
    """
    OAuth token grant type.
    """
    AUTHORIZATION_CODE = 'authorization_code'
    REFRESH_TOKEN = 'refresh_token'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OAuthGrantType

class OptionContractType(Enum):
    """
    Type of option.
    """
    ALL = 'ALL'    # default; 
    CALL = 'CALL'
    PUT = 'PUT'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OptionContractType

class OptionRangeType(Enum):
    """
    Type of option strike range.
    """
    ALL = 'ALL'     # default; 
    ITM = 'ITM'     # In The Money
    NTM = 'NTM'     # Near The Money
    OTM = 'OTM'     # Out of The Money
    SAK = 'SAK'     # Strikes Above Market
    SBK = 'SBK'     # Strikes Below Market
    SNK = 'SNK'     # Strikes Near Market
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OptionRangeType

class OrderDirection(Enum):
    """
    Which way does the order flow.
    """
    BUY = 'BUY'
    SELL = 'SELL'
    BUY_TO_OPEN = 'BUY_TO_OPEN'
    SELL_TO_OPEN = 'SELL_TO_OPEN'
    BUY_TO_CLOSE = 'BUY_TO_CLOSE'
    SELL_TO_CLOSE = 'SELL_TO_CLOSE'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OrderDirection

class OrderDuration(Enum):
    """
    How long an order stays in effect.
    """
    DAY              = 'DAY'
    GOOD_TILL_CANCEL = 'GOOD_TILL_CANCEL'
    FILL_OR_KILL     = 'FILL_OR_KILL'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OrderDuration

class OrderSession(Enum):
    """
    When an order takes effect.
    """
    NORMAL   = 'NORMAL'
    AM       = 'AM'
    PM       = 'PM'
    SEAMLESS = 'SEAMLESS'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OrderSession

class OrderStatus(Enum):
    """
    Status of an order.
    """
    UNINITIALIZED          = ''
    AWAITING_PARENT_ORDER  = 'AWAITING_PARENT_ORDER'
    AWAITING_CONDITION     = 'AWAITING_CONDITION'
    AWAITING_MANUAL_REVIEW = 'AWAITING_MANUAL_REVIEW'
    ACCEPTED               = 'ACCEPTED'
    AWAITING_UR_OUT        = 'AWAITING_UR_OUT'
    PENDING_ACTIVATION     = 'PENDING_ACTIVATION'
    QUEUED                 = 'QUEUED'
    WORKING                = 'WORKING'
    REJECTED               = 'REJECTED'
    PENDING_CANCEL         = 'PENDING_CANCEL'
    CANCELED               = 'CANCELED'
    PENDING_REPLACE        = 'PENDING_REPLACE'
    REPLACED               = 'REPLACED'
    FILLED                 = 'FILLED'
    EXPIRED                = 'EXPIRED'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OrderStatus

class OrderStrategyType(Enum):
    """
    Type of order.
    """
    SINGLE = 'SINGLE'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OrderStrategyType

class OrderType(Enum):
    """
    Type of order.
    """
    MARKET              = 'MARKET'
    LIMIT               = 'LIMIT'
    STOP                = 'STOP'
    STOP_LIMIT          = 'STOP_LIMIT'
    TRAILING_STOP       = 'TRAILING_STOP'
    MARKET_ON_CLOSE     = 'MARKET_ON_CLOSE'
    EXERCISE            = 'EXERCISE'
    TRAILING_STOP_LIMIT = 'TRAILING_STOP_LIMIT'
    NET_DEBIT           = 'NET_DEBIT'
    NET_CREDIT          = 'NET_CREDIT'
    NET_ZERO            = 'NET_ZERO'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: OrderType

class PeriodType(Enum):
    """
    Total period of time over which to capture history.
    """
    DAY = 'day'         # default; supports FrequencyType.MINUTELY only
    MONTH = 'month'     # supports Frequency.{DAILY, WEEKLY(default)}
    YEAR = 'year'       # supports Frequency.{DAILY, WEEKLY, MONTHLY(default)}
    YTD = 'ytd'         # supports Frequency.{DAILY, WEEKLY(default)}
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: PeriodType

def build_oauth_url(app_key, app_redirect_url):
    """
    Construct the URL that will authenticate the user and grant access to the given application.

    Parameters
    ----------
    app_key: str
        App key.

    app_redirect_url: str
        App redirect URL.

    Returns
    -------
    str: 
        Authentication URL.
    """
    return tda.oauth_url_template.format(
        urllib.parse.quote(app_redirect_url),
        urllib.parse.quote(app_key),
      )
#END: build_oauth_url

def cache_oauth_tokens(oauth_token_dict):
    """
    Store OAuth tokens.

    Parameters
    ----------
    oauth_token_dict: dict
        OAuth token specifications.
    """
    with open(tda.oauth_cache_path, 'wb') as f:
        pickle.dump(oauth_token_dict, f)
#END: cache_oauth_tokens

def get_history(symbol, period_type, period, frequency_type, frequency):
    """
    Fetch price/volume history.

    Parameters
    ----------
    symbol : str
        Ticker for which to get history.

    period_type : PeriodType
        Type of period of time over which to capture history.

    period : int
        Total period of time over which to capture history.

    frequency_type : FrequencyType
        Type of aggregation period.

    frequency : int
        Aggregation period.

    Returns
    -------
    [dict]
        List of OHLC and volume data.
    """
    access_token = _get_access_token()
    try:
        response = requests.get(
                'https://api.tdameritrade.com/v1/marketdata/{}/pricehistory?periodType={}&period={}&frequencyType={}&frequency={}&endDate={}'.format(
                symbol,
                period_type.value,
                period,
                frequency_type.value,
                frequency,
                int(datetime.datetime.now().timestamp()*1000),
                ),
            headers={
                'Authorization': 'Bearer {}'.format(access_token),
            },
            timeout=tda.requests_timeout
            )
    except Exception as e:
        logger.error(str(e))
        return None
    if not response.ok:
        logger.error(response.reason)
        return None
    raw_history = response.json()
    history = [{field: field.typecast(raw_bar[raw_field])
        for raw_field in raw_bar
            if (field := tda.ChartBarField(raw_field)) is not tda.ChartBarField.UNSUPPORTED
        }
        for raw_bar in raw_history['candles'][::-1]
    ]
    return history
#END: get_history

def get_option_chains(symbols, contract_type=OptionContractType.ALL, strike_count=0, range_type=OptionRangeType.ALL, from_date=None, to_date=None):
    """
    Fetch option chains on the underlying symbol.

    Parameters
    ----------
    symbols : str or [str]
        Ticker or tickers for which to get quotes.


    contract_type : OptionContractType (default: OptionContractType.ALL)
        Type of option contract to fetch.

    strike_count : int (default: all strikes)
        Number of strikes around market to return.

    range_type : OptionRangeType (default: OptionRangeType.ALL)
        Type of option contract to fetch.

    from_date: datetime (default: earliest available)
        Return expirations after and on this date.

    to_date: datetime (default: latest available)
        Return expirations before and on this date.

    Returns
    -------
    dict
        Option chains keyed by contract type, expiration, and then strike.
    """
    access_token = _get_access_token()
    if isinstance(symbols, str): # single ticker
        symbols = [symbols]
    strike_count_str = '&strikeCount={}'.format(str(strike_count)) if strike_count > 0 else ''
    from_date_str = '&fromDate={:%Y-%m-%d}'.format(from_date) if from_date else ''
    to_date_str = '&toDate={:%Y-%m-%d}'.format(to_date) if to_date else ''
    option_chain_dict = {}
    for symbol in symbols:
        try:
            response = requests.get(
                    'https://api.tdameritrade.com/v1/marketdata/chains?symbol={}&contractType={}{}&range={}{}{}&optionType=S'.format(
                    symbol,
                    contract_type.value,
                    strike_count_str,
                    range_type.value,
                    from_date_str,
                    to_date_str,
                    int(datetime.datetime.now().timestamp()*1000),
                    ),
                headers={
                    'Authorization': 'Bearer {}'.format(access_token),
                },
                timeout=tda.requests_timeout
                )
        except Exception as e:
            logger.error(str(e))
            return None
        if not response.ok:
            logger.error(response.reason)
            return None
        raw_option_chain = response.json()
        #XXX: PM-settlement is usually preferred but consider supporting AM-settlement.
        contracts = [strike[0] if (strike[0]['settlementType'] == 'P' or strike[0]['settlementType'] == ' ') else strike[-1]
            for contract_map in ('callExpDateMap', 'putExpDateMap')
                for _, opex in raw_option_chain[contract_map].items()
                    for _, strike in opex.items()
        ]
        option_chain_dict[symbol] = [{field: field.typecast(raw_value)
            for raw_field, raw_value in contract.items()
                if (field := tda.OptionContractField(raw_field)) is not tda.OptionContractField.UNSUPPORTED
            }
            for contract in contracts
        ]
    return option_chain_dict
#END: get_option_chains

def get_quotes(symbols):
    """
    Fetch real-time quotes of the given symbols.

    Parameters
    ----------
    symbols : str or [str]
        Ticker or tickers for which to get quotes.

    Returns
    -------
    dict
        Quote data keyed by symbol.
    """
    access_token = _get_access_token()
    url_str = 'https://api.tdameritrade.com/v1/marketdata/{}quotes{}'
    if isinstance(symbols, str): # single ticker
        url_str = url_str.format(symbols + '/', '')
    elif isinstance(symbols, list): # list of tickers
        symbols_str = urllib.parse.quote(','.join(symbols))
        url_str = url_str.format('', '?symbol=' + symbols_str)
    try:
        response = requests.get(
            url_str,
            headers={
                'Authorization': 'Bearer {}'.format(access_token),
            },
            timeout=tda.requests_timeout
            )
    except Exception as e:
        logger.error(str(e))
        return None
    if not response.ok:
        logger.error(response.reason)
        return None
    raw_quote_dict = response.json()
    quote_dict = {ticker: {field: field.typecast(raw_quote[raw_field])
        for raw_field in raw_quote
            if (field := tda.QuoteField(raw_field)) is not tda.QuoteField.UNSUPPORTED
        }
        for ticker, raw_quote in raw_quote_dict.items()
    }
    return quote_dict
#END: get_quotes

def post_order(account_id, new_order_specs):
    """
    Post a new order.

    Parameters
    ----------
    account_id: int
        Account ID.

    new_order_specs: dict
        New order specifications.

    Returns
    -------
    dict:
        Response to POST request. None if error.
    """
    logger.debug(new_order_specs)
    access_token = _get_access_token()
    try:
        response = requests.post(
            'https://api.tdameritrade.com/v1/accounts/{}/orders'.format(account_id),
            headers={
                'Authorization': 'Bearer {}'.format(access_token),
                'Content-Type': 'application/json',
            },
            json=new_order_specs,
            timeout=tda.requests_timeout
            )
    except Exception as e:
        logger.error(str(e))
        return None
    if not response.ok:
        logger.error(response.reason)
        logger.error(response.content)
        return None
    logger.info('POST order: ' + str(new_order_specs))
    return response
#END: post_order

def renew_oauth_tokens(app_key, app_redirect_url, code_encoded):
    """
    Renew OAuth tokens.

    Parameters
    ----------
    app_key: str
        App key.

    app_redirect_url: str
        App redirect URL.

    code_encoded: str
        URL-encoded response code received from authentication.

    Returns
    -------
    dict:
        OAuth token specifications. None if error.
    """
    oauth_token_dict = {
        'client_id': '{}@AMER.OAUTHAP'.format(app_key),
        'created_at': datetime.datetime.now(pytz.UTC),
    }
    code = urllib.parse.unquote(code_encoded)
    oauth_token_dict.update(_post_token(
            grant_type=OAuthGrantType.AUTHORIZATION_CODE,
            client_id=oauth_token_dict['client_id'],
            redirect_url=app_redirect_url, 
            code=code,
            ))
    oauth_token_dict['access_token_expires_at'] = oauth_token_dict['created_at'] + datetime.timedelta(seconds=oauth_token_dict['expires_in'])
    oauth_token_dict['refresh_token_expires_at'] = oauth_token_dict['created_at'] + datetime.timedelta(seconds=oauth_token_dict['refresh_token_expires_in'])
    return oauth_token_dict
#END: renew_oauth_tokens


#################
# LOW-LEVEL API #
#################

def _get_access_token():
    """
    Fetch access token from cache or get a new one.

    Returns
    -------
    str
        Access token.
    """
    if tda.oauth_access_token is None or tda.oauth_access_token_expires_at is None:
        oauth_token_dict = {}
        with open(tda.oauth_cache_path, 'rb') as f:
            oauth_token_dict.update(pickle.load(f))
        tda.oauth_access_token = oauth_token_dict['access_token']
        tda.oauth_access_token_expires_at = oauth_token_dict['access_token_expires_at']
    now = datetime.datetime.now(pytz.UTC)
    if now >= tda.oauth_access_token_expires_at:
        tda.oauth_access_token, tda.oauth_access_token_expires_at = _renew_access_token()
    return tda.oauth_access_token
#END: _get_access_token

def _get_user_principals(streamer_subscription_keys=False, streamer_connection_info=False, preferences=False, surrogate_ids=False):
    """
    Fetch various user principals details.

    Parameters
    ----------
    streamer_subscription_keys: bool (default: False)
        Return streamer subscription keys.

    streamer_connection_info: bool (default: False)
        Return streamer connection details.

    preferences: bool (default: False)
        Return user preferences.

    surrogate_ids: bool (default: False)
        Return surrogate IDs.

    Returns
    -------
    dict:
        User principals details.
    """
    access_token = _get_access_token()
    fields = list(f for f in [
        'streamerSubscriptionKeys' if streamer_subscription_keys else None,
        'streamerConnectionInfo'   if streamer_connection_info else None,
        'preferences'              if preferences else None,
        'surrogateIds'             if surrogate_ids else None,
    ] if f is not None)
    query_str = '?fields={}'.format(urllib.parse.quote(','.join(fields))) if len(fields) > 0 else ''
    try:
        response = requests.get(
                'https://api.tdameritrade.com/v1/userprincipals' + query_str,
                headers={
                    'Authorization': 'Bearer {}'.format(access_token),
                },
                timeout=tda.requests_timeout
            )
    except Exception as e:
        logger.error(str(e))
        return None
    if not response.ok:
        logger.error(response.reason)
        return None
    return response.json()
#END: _get_user_principals

def _post_token(grant_type, client_id, redirect_url=None, code=None, refresh_token=None):
    """
    Create a new OAuth token set.

    Parameters
    ----------
    grant_type: OAuthGrantType(Enum)
        OAuth scheme.

    client_id: str
        OAuth user ID.

    redirect_url: str (default: None)
        OAuth redirect URL.

    code: str (default: None)
        Response code received from authentication. Do not URL-encode.

    refresh_token: str (default: None)
        OAuth refresh token. Only needed when renewing the access token.

    Returns
    -------
    dict:
        OAuth token specifications. None if error.

    Raises
    ------
    ValueError:
        Invalid grant parameters.
    """
    data = {
        'grant_type': grant_type.value,
        'client_id': client_id,
    }
    if grant_type == OAuthGrantType.AUTHORIZATION_CODE and refresh_token is None:
        data.update({
            'access_type': 'offline',
            'redirect_uri': redirect_url,
            'code': code,
        })
    elif grant_type == OAuthGrantType.REFRESH_TOKEN and refresh_token is not None:
        data.update({
            'refresh_token': refresh_token,
        })
    else:
        logger.critical('Invalid POST token parameters.')
        raise ValueError('Invalid POST token parameters.')
    try:
        response = requests.post(
            'https://api.tdameritrade.com/v1/oauth2/token',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            data=data,
            timeout=tda.requests_timeout,
            )
    except:
        logger.exception('Failed to POST token.')
        return None
    if not response.ok:
        logger.error(response.reason)
        return None
    return response.json()
#END: _post_token

def _renew_access_token():
    """
    Use current refresh token to obtain a new access token.
    
    Returns
    -------
    str:
        Access token

    datetime:
        Access token expiration.

    Raises
    ------
    RuntimeError:
        Invalid refresh token.
    """
    oauth_token_dict = {}
    try:
        with open(tda.oauth_cache_path, 'rb') as f:
            oauth_token_dict.update(pickle.load(f))
    except:
        logger.exception('Failed to load OAuth token cache: {}'.format(tda.oauth_cache_path))
        raise RuntimeError('Missing {}.'.format(tda.oauth_cache_path))
    if not('refresh_token' in oauth_token_dict and 'refresh_token_expires_at' in oauth_token_dict):
        logger.critical('Missing refresh token.')
        raise RuntimeError('Missing refresh token. Please run `renew_tokens.py`.')
    now = datetime.datetime.now(pytz.UTC)
    if now >= oauth_token_dict['refresh_token_expires_at']:
        logger.critical('Refresh token expired.')
        raise RuntimeError('Refresh token expired. Please run `renew_tokens.py`.')
    token_patch = None
    while token_patch is None:
        token_patch = _post_token(
            grant_type=OAuthGrantType.REFRESH_TOKEN,
            client_id=oauth_token_dict['client_id'],
            refresh_token=oauth_token_dict['refresh_token'],
        )
    oauth_token_dict.update(token_patch)
    oauth_token_dict['access_token_expires_at'] = now + datetime.timedelta(seconds=oauth_token_dict['expires_in'])
    cache_oauth_tokens(oauth_token_dict)
    return oauth_token_dict['access_token'], oauth_token_dict['access_token_expires_at']
#END: _renew_access_token
