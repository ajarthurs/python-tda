"""
TDA streaming module.
Supports streaming from TDA's websocket server.
"""

import asyncio
import dateutil.parser
import json
import logging
import tda
import tda.api
import urllib
import websockets
from enum import Enum, auto

logger = logging.getLogger(__name__)

##################
# WEBSOCKETS API #
##################

class WSAccountActivityField(Enum):
    """
    ACCT_ACTIVITY numeric field map used when subscribing.
    """
    SUBSCRIPTION_KEY = 0
    ACCOUNT_ID       = 1
    MESSAGE_TYPE     = 2
    MESSAGE_DATA     = 3
#END: WSAccountActivityField

class WSChartEquityField(Enum):
    """
    CHART_EQUITY numeric field map used when subscribing.
    """
    SYMBOL      = 0
    OPEN_PRICE  = 1
    HIGH_PRICE  = 2
    LOW_PRICE   = 3
    CLOSE_PRICE = 4
    VOLUME      = 5
    SEQUENCE    = 6
    TIMESTAMP   = 7
#END: WSChartEquityField

class WSCommand(Enum):
    """
    Websocket-based command.
    """
    # ADMIN commands.
    LOGIN  = 'LOGIN'
    LOGOUT = 'LOGOUT'
    QOS    = 'QOS'
    # Common commands.
    SUBS   = 'SUBS'
    UNSUBS = 'UNSUBS'

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: WSCommand

class WSOptionContractField(Enum):
    """
    OPTION numeric field map used when subscribing.
    """
    SYMBOL        = 0
    DESCRIPTION   = 1
    BID_PRICE     = 2
    ASK_PRICE     = 3
    LAST_PRICE    = 4
    MARK_PRICE    = 41
    VOLUME        = 8
    OPEN_INTEREST = 9
    VOLATILITY    = 10
    BID_SIZE      = 20
    ASK_SIZE      = 21
    LAST_SIZE     = 22
    DELTA         = 32
#END: WSOptionContractField

class WSQOSLevel(Enum):
    """
    Quality of service.
    """
    EXPRESS   = 0
    REAL_TIME = 1 # default value for http binary protocol
    FAST      = 2 # default value for websocket and http asynchronous protocol
    MODERATE  = 3
    SLOW      = 4
    DELAYED   = 5
    # Alias named by time-delta (milliseconds).
    D500MS    = 0
    D750MS    = 1
    D1000MS   = 2
    D1500MS   = 3
    D3000MS   = 4
    D5000MS   = 5
#END: WSQOSLevel

class WSQuoteField(Enum):
    """
    QUOTE numeric field map used when subscribing.
    """
    SYMBOL          = 0
    BID_PRICE       = 1
    ASK_PRICE       = 2
    LAST_PRICE      = 3
    MARK_PRICE      = 49
    BID_SIZE        = 4
    ASK_SIZE        = 5
    LAST_SIZE       = 9
    VOLATILITY      = 24
    DESCRIPTION     = 25
    QUOTE_TIMESTAMP = 50
    LAST_TIMESTAMP  = 51
#END: WSQuoteField

class WSService(Enum):
    """
    Websocket-based service.
    """
    ACCT_ACTIVITY   = 'ACCT_ACTIVITY'
    ADMIN           = 'ADMIN'
    CHART_EQUITY    = 'CHART_EQUITY'
    OPTION          = 'OPTION'
    QUOTE           = 'QUOTE'
    TIMESALE_EQUITY = 'TIMESALE_EQUITY'

    @classmethod
    def _missing_(cls, name):
        return tda._enum_case_insensitive_search_by_value(cls, name)
#END: WSService

class WSTimesaleEquityField(Enum):
    """
    TIMESALE_EQUITY numeric field map used when subscribing.
    """
    SYMBOL        = 0
    TRADE_TIME    = 1
    LAST_PRICE    = 2
    LAST_SIZE     = 3
    LAST_SEQUENCE = 4
#END: WSTimesaleEquityField

def ws_connect(account_id, qos):
    """
    Setup a websockets connection to the given account ID.

    Parameters
    ----------
    account_id: str
        TD Ameritrade account ID.

    qos: tda.streaming.WSQOSLevel(Enum)
        Quality of service.

    Returns
    -------
    dict(dict, dict, WebSocketClientProtocol):
        User principal specifications, selected account details, and client websocket handle. Returns None on error.
    """
    user_principal_dict = tda.api._get_user_principals(
            streamer_subscription_keys=True,
            streamer_connection_info=True,
            )
    if user_principal_dict is None:
        logger.error('Failed to retrieve user principals.')
        return None
    account = None
    for listed_account in user_principal_dict['accounts']:
        if listed_account['accountId'] == account_id:
            account = listed_account
            break
    if account is None:
        logger.error('Account ID "{}" does not match user principals.'.format(account_id))
        return None
    token_timestamp = dateutil.parser.parse(user_principal_dict['streamerInfo']['tokenTimestamp'])
    token_timestamp_ms = int(token_timestamp.timestamp() * 1000)
    credentials = {
        'userid': account['accountId'],
        'token': user_principal_dict['streamerInfo']['token'],
        'company': account['company'],
        'segment': account['segment'],
        'cddomain': account['accountCdDomainId'],
        'usergroup': user_principal_dict['streamerInfo']['userGroup'],
        'accesslevel': user_principal_dict['streamerInfo']['accessLevel'],
        'authorized': 'Y',
        'timestamp': token_timestamp_ms,
        'appid': user_principal_dict['streamerInfo']['appId'],
        'acl': user_principal_dict['streamerInfo']['acl'],
    }
    login_request = {
        'requests': [
        {
            'service': WSService.ADMIN.value,
            'command': WSCommand.LOGIN.value,
            'requestid': 0,
            'account': account['accountId'],
            'source': user_principal_dict['streamerInfo']['appId'],
            'parameters': {
                'credential': urllib.parse.urlencode(credentials),
                'token': user_principal_dict['streamerInfo']['token'],
                'version': '1.0',
                'qoslevel': qos.value,
            }
        },
        ],
    }
    uri = 'wss://' + user_principal_dict['streamerInfo']['streamerSocketUrl'] + '/ws'
    async def _connect():
        ws = await websockets.connect(uri, ssl=True)
        await ws.send(json.dumps(login_request))
        response = {}
        while 'response' not in response:
            response = json.loads(await ws.recv())
        logger.debug(response)
        if response['response'][0]['content']['code'] != 0:
            logger.error('Failed to LOGIN: {}'.format(response))
        return ws
    ws = asyncio.get_event_loop().run_until_complete(_connect())
    return {
        'account': account,
        'principal': user_principal_dict,
        'websocket': ws,
        'qos': qos,
        'next_request_id': 1,
    }
#END: ws_connect

def ws_disconnect(ws):
    """
    Shutdown current websockets connection.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.
    """
    logout_request = {
        'requests': [
        {
            'service': WSService.ADMIN.value,
            'command': WSCommand.LOGOUT.value,
            'requestid': ws['next_request_id'],
            'account': ws['account']['accountId'],
            'source': ws['principal']['streamerInfo']['appId'],
            'parameters': {},
        },
        ],
    }
    async def _disconnect():
        await ws['websocket'].send(json.dumps(logout_request))
        response = {}
        while 'response' not in response:
            response = json.loads(await ws['websocket'].recv())
        logger.debug(response)
        if response['response'][0]['content']['code'] != 0:
            logger.error('Failed to LOGOUT: {}'.format(response))
            return
        await ws['websocket'].close()
    asyncio.get_event_loop().run_until_complete(_disconnect())
#END: ws_disconnect

def ws_listen(ws, flags):
    """
    Process incoming messages from TDA via a websocket.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    flags: dict
        To terminate and return, set flags['done'] to True.
    """
    async def _listen():
        try:
            response = await ws['websocket'].recv()
        except websockets.exceptions.ConnectionClosedError:
            return None
        return json.loads(response)
    while not('done' in flags and flags['done']):
        response = asyncio.get_event_loop().run_until_complete(_listen())
        if response is None:
            #ws.update(ws_connect(ws['account']['accountId'], ws['qos']))
            ws_prime = None
            while not(ws_prime or ('done' in flags and flags['done'])):
                ws_prime = ws_connect(ws['account']['accountId'], ws['qos'])
            if ws_prime:
                ws.update(ws_prime)
                _ws_resubscribe(ws)
            continue
        if 'notify' in response: # Heartbeat message. Skip.
            continue
        if 'response' in response: # Confirmation message. Check status.
            if not(len(response['response']) == 1 and 'content' in response['response'][0]):
                continue
            content = response['response'][0]['content']
            if content['code'] != 0:
                if 'msg' in content:
                    logger.error(content['msg'])
                else:
                    logger.error('Bad response: {}'.format(response))
            continue
        if 'data' not in response: # Unknown message. Skip.
            continue
        for data in response['data']:
            if 'service' not in data:
                continue
            service = WSService(data['service'])
            if service.value not in ws:
                continue
            distilled_data = _ws_distill_data(data)
            if distilled_data is None:
                continue
            for cb_idx, cb_function in enumerate(ws[service.value]['cb_functions']):
                cb_function(
                    distilled_data,
                    ws[service.value]['cb_data'][cb_idx],
                )
#END: ws_listen

def ws_subscribe_to_acct_activity(ws, cb_functions, cb_data=None):
    """
    Subscribe callback to account activity such as order updates.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    cb_functions: [function]
        Functions that will handle updates from service.

    cb_data: [any] (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    _ws_subscribe(
            ws,
            WSService.ACCT_ACTIVITY,
            fields = [
                WSAccountActivityField.ACCOUNT_ID,
                WSAccountActivityField.MESSAGE_TYPE,
                WSAccountActivityField.MESSAGE_DATA,
            ],
            cb_functions = cb_functions,
            cb_data = cb_data,
            )
#END: ws_subscribe_to_acct_activity

def ws_subscribe_to_chart_equity(ws, symbols, cb_functions, cb_data=None):
    """
    Subscribe callback to equity chart (1-minute).

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    symbols: [str]
        List of symbols.

    cb_functions: [function]
        Functions that will handle updates from service.

    cb_data: any (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    _ws_subscribe(
            ws,
            WSService.CHART_EQUITY,
            fields = [
                WSChartEquityField.SYMBOL,
                WSChartEquityField.OPEN_PRICE,
                WSChartEquityField.HIGH_PRICE,
                WSChartEquityField.LOW_PRICE,
                WSChartEquityField.CLOSE_PRICE,
                WSChartEquityField.VOLUME,
                WSChartEquityField.SEQUENCE,
                WSChartEquityField.TIMESTAMP,
            ],
            cb_functions = cb_functions,
            cb_data = cb_data,
            symbols = symbols,
            process_first_data = True,
            )
#END: ws_subscribe_to_chart_equity

def ws_subscribe_to_option(ws, symbols, cb_functions, cb_data=None):
    """
    Subscribe callback to equity option quotes.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    symbols: [str]
        List of option contract symbols.

    cb_functions: [function]
        Functions that will handle updates from service.

    cb_data: any (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    _ws_subscribe(
            ws,
            WSService.OPTION,
            fields = [
                WSOptionContractField.SYMBOL,
                WSOptionContractField.BID_PRICE,
                WSOptionContractField.ASK_PRICE,
                WSOptionContractField.LAST_PRICE,
                WSOptionContractField.VOLUME,
                WSOptionContractField.OPEN_INTEREST,
                WSOptionContractField.VOLATILITY,
                WSOptionContractField.BID_SIZE,
                WSOptionContractField.ASK_SIZE,
                WSOptionContractField.LAST_SIZE,
                WSOptionContractField.DELTA,
                WSOptionContractField.MARK_PRICE,
            ],
            cb_functions = cb_functions,
            cb_data = cb_data,
            symbols = symbols,
            process_first_data = True,
            )
#END: ws_subscribe_to_option

def ws_subscribe_to_quote(ws, symbols, cb_functions, cb_data=None):
    """
    Subscribe callback to equity quotes.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    symbols: [str]
        List of symbols.

    cb_functions: [function]
        Functions that will handle updates from service.

    cb_data: any (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    _ws_subscribe(
            ws,
            WSService.QUOTE,
            fields = [
                WSQuoteField.SYMBOL,
                WSQuoteField.BID_PRICE,
                WSQuoteField.ASK_PRICE,
                WSQuoteField.LAST_PRICE,
                WSQuoteField.MARK_PRICE,
                WSQuoteField.BID_SIZE,
                WSQuoteField.ASK_SIZE,
                WSQuoteField.LAST_SIZE,
                WSQuoteField.VOLATILITY,
                WSQuoteField.QUOTE_TIMESTAMP,
                WSQuoteField.LAST_TIMESTAMP,
            ],
            cb_functions = cb_functions,
            cb_data = cb_data,
            symbols = symbols,
            process_first_data = True,
            )
#END: ws_subscribe_to_quote

def ws_subscribe_to_timesale_equity(ws, symbols, cb_functions, cb_data=None):
    """
    Subscribe callback to equity time&sales.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    symbols: [str]
        List of symbols.

    cb_functions: [function]
        Functions that will handle updates from service.

    cb_data: any (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    _ws_subscribe(
            ws,
            WSService.TIMESALE_EQUITY,
            fields = [
                WSTimesaleEquityField.SYMBOL,
                WSTimesaleEquityField.TRADE_TIME,
                WSTimesaleEquityField.LAST_PRICE,
                WSTimesaleEquityField.LAST_SIZE,
                WSTimesaleEquityField.LAST_SEQUENCE,
            ],
            cb_functions = cb_functions,
            cb_data = cb_data,
            symbols = symbols,
            process_first_data = True,
            )
#END: ws_subscribe_to_timesale_equity

#################
# LOW-LEVEL API #
#################

def _ws_distill_data(data):
    """
    Distill data sent from TDA according to service.

    Parameters
    ----------
    data: dict
        Data sent from TDA.

    Returns
    -------
    dict:
        Distilled data from TDA.
    """
    service = WSService(data['service'])
    data_prime = {}
    if 'content' not in data or len(data['content']) == 0:
        logger.error('Missing expected content: {}'.format(data))
        return None
    content = data['content']
    if service == WSService.ACCT_ACTIVITY:
        content = content[0]
        message_type = str(content[str(WSAccountActivityField.MESSAGE_TYPE.value)]).strip().upper()
        if message_type == 'ORDERENTRYREQUEST': # OrderEntryRequest: order submitted.
            data_prime['status'] = tda.OrderStatus.QUEUED.value
        elif message_type == 'ORDERFILL': # OrderFill: order filled.
            data_prime['status'] = tda.OrderStatus.FILLED.value
        elif message_type == 'UROUT': # UROUT: order canceled.
            data_prime['status'] = tda.OrderStatus.CANCELED.value
        elif message_type == 'ORDERREJECTION': # OrderRejection: order rejected.
            data_prime['status'] = tda.OrderStatus.REJECTED.value
        else:
            return None
        data_prime['accountId'] = content[str(WSAccountActivityField.ACCOUNT_ID.value)]
        message_xml = content[str(WSAccountActivityField.MESSAGE_DATA.value)]
        message_root = ET.fromstring(message_xml)
        order_id_element = message_root.find('{*}Order/{*}OrderKey')
        data_prime['orderId'] = order_id_element.text
    elif service == WSService.CHART_EQUITY:
        data_prime = []
        field_map = {
            WSChartEquityField.TIMESTAMP   : tda.ChartBarField.TIMESTAMP,
            WSChartEquityField.OPEN_PRICE  : tda.ChartBarField.OPEN_PRICE,
            WSChartEquityField.HIGH_PRICE  : tda.ChartBarField.HIGH_PRICE,
            WSChartEquityField.LOW_PRICE   : tda.ChartBarField.LOW_PRICE,
            WSChartEquityField.CLOSE_PRICE : tda.ChartBarField.CLOSE_PRICE,
            WSChartEquityField.VOLUME      : tda.ChartBarField.VOLUME,
        }
        for bar in content:
            bar_prime = {field: field.typecast(bar[str(ws_field.value)])
                for ws_field, field in field_map.items()
                    if str(ws_field.value) in bar
            }
            bar_prime[tda.ChartBarField.SYMBOL] = bar['key']
            data_prime.append(bar_prime)
    elif service == WSService.OPTION:
        data_prime = []
        for option in content:
            field_map = {
                WSOptionContractField.BID_PRICE     : tda.OptionContractField.BID_PRICE,
                WSOptionContractField.ASK_PRICE     : tda.OptionContractField.ASK_PRICE,
                WSOptionContractField.LAST_PRICE    : tda.OptionContractField.LAST_PRICE,
                WSOptionContractField.MARK_PRICE    : tda.OptionContractField.MARK_PRICE,
                WSOptionContractField.VOLUME        : tda.OptionContractField.VOLUME,
                WSOptionContractField.VOLATILITY    : tda.OptionContractField.VOLATILITY,
                WSOptionContractField.OPEN_INTEREST : tda.OptionContractField.OPEN_INTEREST,
                WSOptionContractField.BID_SIZE      : tda.OptionContractField.BID_SIZE,
                WSOptionContractField.ASK_SIZE      : tda.OptionContractField.ASK_SIZE,
                WSOptionContractField.LAST_SIZE     : tda.OptionContractField.LAST_SIZE,
                WSOptionContractField.DELTA         : tda.OptionContractField.DELTA,
            }
            option_prime = {field: field.typecast(option[str(ws_field.value)])
                for ws_field, field in field_map.items()
                    if str(ws_field.value) in option
            }
            option_prime[tda.OptionContractField.SYMBOL] = option['key']
            data_prime.append(option_prime)
    elif service == WSService.QUOTE:
        data_prime = []
        for quote in content:
            field_map = {
                WSQuoteField.BID_PRICE       : tda.QuoteField.BID_PRICE,
                WSQuoteField.ASK_PRICE       : tda.QuoteField.ASK_PRICE,
                WSQuoteField.LAST_PRICE      : tda.QuoteField.LAST_PRICE,
                WSQuoteField.MARK_PRICE      : tda.QuoteField.MARK_PRICE,
                WSQuoteField.BID_SIZE        : tda.QuoteField.BID_SIZE,
                WSQuoteField.ASK_SIZE        : tda.QuoteField.ASK_SIZE,
                WSQuoteField.LAST_SIZE       : tda.QuoteField.LAST_SIZE,
                WSQuoteField.VOLATILITY      : tda.QuoteField.VOLATILITY,
                WSQuoteField.QUOTE_TIMESTAMP : tda.QuoteField.QUOTE_TIMESTAMP,
                WSQuoteField.LAST_TIMESTAMP  : tda.QuoteField.LAST_TIMESTAMP,
            }
            quote_prime = {field: field.typecast(quote[str(ws_field.value)])
                for ws_field, field in field_map.items()
                    if str(ws_field.value) in quote
            }
            quote_prime[tda.QuoteField.SYMBOL] = quote['key']
            data_prime.append(quote_prime)
    elif service == WSService.TIMESALE_EQUITY:
        data_prime = []
        for ts in content:
            ts_prime = {}
            ts_prime[WSTimesaleEquityField.SYMBOL] = ts['key']
            ts_prime[WSTimesaleEquityField.TRADE_TIME]    = ts[str(WSTimesaleEquityField.TRADE_TIME.value)]
            ts_prime[WSTimesaleEquityField.LAST_PRICE]    = ts[str(WSTimesaleEquityField.LAST_PRICE.value)]
            ts_prime[WSTimesaleEquityField.LAST_SIZE]     = int(ts[str(WSTimesaleEquityField.LAST_SIZE.value)])
            ts_prime[WSTimesaleEquityField.LAST_SEQUENCE] = ts[str(WSTimesaleEquityField.LAST_SEQUENCE.value)]
            data_prime.append(ts_prime)
    else:
        return None
    return data_prime
#END: _ws_distill_data

def _ws_resubscribe(ws):
    """
    Resubscribe callbacks to a websocket service.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.
    """
    for service in (WSService.ACCT_ACTIVITY, WSService.CHART_EQUITY, WSService.OPTION, WSService.QUOTE):
        if service.value not in ws:
            continue
        params = ws[service.value]
        _ws_subscribe(ws, service, params['fields'], params['cb_functions'], params['cb_data'], params['symbols'], params['process_first_data'])
#END: _ws_resubscribe

def _ws_subscribe(ws, service, fields, cb_functions, cb_data=None, symbols=None, process_first_data=False):
    """
    Subscribe callback to a websocket service.

    Parameters
    ----------
    ws: dict
        Client websocket handle produced by `ws_connect`.

    service: WSService
        Which service to subscribe to.

    fields: [Enum]
        List of fields to receive from service.

    cb_functions: [function]
        Functions that will handle updates from service.

    cb_data: any (optional)
        Additional data to pass to the callback functions, respective of order.

    symbols: [str] (optional)
        List of symbols if applicable.

    #XXX: Not needed and dispatches data to the wrong callbacks. Remove???
    process_first_data: bool (default: False)
        Set to True to invoke callbacks on first data response.
    """
    logger.debug(service)
    logger.debug(cb_functions)
    field_str = ','.join(str(field.value) for field in fields)
    key_str = ws['principal']['streamerSubscriptionKeys']['keys'][0]['key'] if symbols is None else ','.join(symbols)
    subscribe_request = {
        'requests': [
        {
            'service': service.value,
            'command': WSCommand.SUBS.value,
            'requestid': 1,
            'account': ws['account']['accountId'],
            'source': ws['principal']['streamerInfo']['appId'],
            'parameters': {
                'keys': key_str,
                'fields': field_str,
            },
        },
        ],
    }
    async def _subscribe():
        await ws['websocket'].send(json.dumps(subscribe_request))
        response = {}
        while 'response' not in response:
            response = json.loads(await ws['websocket'].recv())
        logger.debug(response)
        if response['response'][0]['content']['code'] != 0:
            logger.error('Failed to SUBS: {}'.format(response))
        response = {}
        while 'data' not in response:
            response = json.loads(await ws['websocket'].recv())
        return response['data'][0]
    data = asyncio.get_event_loop().run_until_complete(_subscribe())
    data_service = WSService(data['service']) if data is not None and 'service' in data else None
    #XXX: Not needed and dispatches data to the wrong callbacks. Remove???
    distilled_data = _ws_distill_data(data)
    ws[service.value] = {
        'fields': fields,
        'symbols': symbols,
        'process_first_data': process_first_data,
        'cb_functions': cb_functions,
        'cb_data': cb_data,
    }
    if process_first_data and distilled_data is not None:
        #logger.debug(distilled_data)
        for cb_idx, cb_function in enumerate(ws[data_service.value]['cb_functions']):
            #if service.value l
            cb_function(
                distilled_data,
                ws[data_service.value]['cb_data'][cb_idx],
                )
#END: _ws_subscribe
