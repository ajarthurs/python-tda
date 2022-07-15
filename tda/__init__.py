"""
TD Ameritrade API and streaming package.
"""

import logging
from enum import Enum, auto

logger = logging.getLogger(__name__)
oauth_access_token = None
oauth_access_token_expires_at = None
oauth_cache_path = '.tda_oauth.p'
oauth_url_template = 'https://auth.tdameritrade.com/auth?response_type=code&redirect_uri={}&client_id={}%40AMER.OAUTHAP'
requests_timeout = 30 #seconds

class ChartBarField(Enum):
    """
    Chart bar fields.
    """
    SYMBOL      = ('symbol', str)
    TIMESTAMP   = ('datetime', int) # in ms since epoch
    OPEN_PRICE  = ('open', float)
    HIGH_PRICE  = ('high', float)
    LOW_PRICE   = ('low', float)
    CLOSE_PRICE = ('close', float)
    VOLUME      = ('volume', int)
    UNSUPPORTED = ('', str)

    def __init__(self, api_key, typecast):
        self.api_key = api_key
        self.typecast = typecast

    @classmethod
    def _missing_(cls, name):
        return _enum_case_insensitive_search_by_typecasted_value(cls, name)
#END: QuoteField

class InstrumentType(Enum):
    """
    Instrument class.
    """
    EQUITY          = 'EQUITY'
    OPTION          = 'OPTION'
    MUTUAL_FUND     = 'MUTUAL_FUND'
    CASH_EQUIVALENT = 'CASH_EQUIVALENT'
    FIXED_INCOME    = 'FIXED_INCOME'
    CURRENCY        = 'CURRENCY'
    INDEX           = 'INDEX'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return _enum_case_insensitive_search_by_value(cls, name)
#END: InstrumentType

class OptionContractField(Enum):
    """
    Option contract field.
    """
    SYMBOL          = ('symbol', str)
    DESCRIPTION     = ('description', str)
    OPEX            = ('expirationDate', int) # in ms since epoch
    SETTLEMENT_TYPE = ('settlementType', str)
    STRIKE          = ('strikePrice', float)
    CONTRACT_TYPE   = ('putCall', str)
    BID_PRICE       = ('bid', float)
    ASK_PRICE       = ('ask', float)
    LAST_PRICE      = ('last', float)
    MARK_PRICE      = ('mark', float)
    VOLUME          = ('volume', int)
    VOLATILITY      = ('volatility', float)
    OPEN_INTEREST   = ('openInterest', int)
    BID_SIZE        = ('bidSize', int)
    ASK_SIZE        = ('askSize', int)
    LAST_SIZE       = ('lastSize', int)
    DELTA           = ('delta', float)
    GAMMA           = ('gamma', float)
    THETA           = ('theta', float)
    UNSUPPORTED = ('', str)

    def __init__(self, api_key, typecast):
        self.api_key = api_key
        self.typecast = typecast

    @classmethod
    def _missing_(cls, name):
        return _enum_case_insensitive_search_by_typecasted_value(cls, name)
#END: OptionContractField

class QuoteField(Enum):
    """
    Quote fields.
    """
    SYMBOL          = ('symbol', str)
    DESCRIPTION     = ('description', str)
    BID_PRICE       = ('bidPrice', float)
    ASK_PRICE       = ('askPrice', float)
    LAST_PRICE      = ('lastPrice', float)
    MARK_PRICE      = ('mark', float)
    BID_SIZE        = ('bidSize', int)
    ASK_SIZE        = ('askSize', int)
    LAST_SIZE       = ('lastSize', int)
    VOLATILITY      = ('volatility', float)
    QUOTE_TIMESTAMP = ('quoteTimestamp', int) # in ms since epoch
    LAST_TIMESTAMP  = ('lastTimestamp', int) # in ms since epoch
    UNSUPPORTED = ('', str)

    def __init__(self, api_key, typecast):
        self.api_key = api_key
        self.typecast = typecast

    @classmethod
    def _missing_(cls, name):
        return _enum_case_insensitive_search_by_typecasted_value(cls, name)
#END: QuoteField


#################
# LOW-LEVEL API #
#################

def _enum_case_insensitive_search_by_typecasted_value(cls, name):
    """
    Return first member whose value matches the given name.

    Parameters
    ----------
    cls: Enum
        Enumeration subclass.

    name: str
        String to search for.

    Returns
    -------
    Enum:
        Matching enumeration member or None.
    """
    #for member in cls:
    #    member_name, _ = member.value
    #    if member_name.lower() == name.strip().lower():
    #        return member
    matches = [member for member in cls if member.value[0].lower() == name.strip().lower()]
    if len(matches) > 0:
        return matches[0]
    else:
        return cls.UNSUPPORTED
#END: _case_insensitive_search_by_typecasted_value

def _enum_case_insensitive_search_by_value(cls, name):
    """
    Return first member whose value matches the given name.

    Parameters
    ----------
    cls: Enum
        Enumeration subclass.

    name: str
        String to search for.

    Returns
    -------
    Enum:
        Matching enumeration member or None.
    """
    matches = [member for member in cls if member.value.lower() == name.strip().lower()]
    #for member in cls:
    #    if member.value.lower() == name.strip().lower():
    #        return member
    if len(matches) > 0:
        return matches[0]
    else:
        return cls.UNSUPPORTED
#END: _enum_case_insensitive_search_by_value
