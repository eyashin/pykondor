import sys
import ctypes
from platform import architecture
from typing import NewType, Callable, List, Any


# module variables
_func = None                # ctype func cast, OS dependent

__lib_bit = lambda: '64' if architecture()[0] == '64bit' else ''
if sys.platform[:5] == "linux" or sys.platform[:3] == "aix":
    # Unix/Linux
    _func = ctypes.CFUNCTYPE
    __lib_name = lambda name: 'lib' + name + __lib_bit() + '.so'
    _rv = ctypes.CDLL(__lib_name("tibrv"))
elif sys.platform == 'win32':
    # Windows
    _func = ctypes.WINFUNCTYPE
    __lib_name = lambda name: name
    _rv = ctypes.CDLL(__lib_name("tibrv"))
else:
    raise SystemError(sys.platform + ' is not supported')


##-----------------------------------------------------------------------------
# CTYPES
# tibrv/types.h
##-----------------------------------------------------------------------------

_c_tibrv_status         = ctypes.c_int32

_c_tibrv_i8                  = ctypes.c_int8
_c_tibrv_u8                  = ctypes.c_uint8
_c_tibrv_i16                 = ctypes.c_int16
_c_tibrv_u16                 = ctypes.c_uint16
_c_tibrv_i32                 = ctypes.c_int32
_c_tibrv_u32                 = ctypes.c_uint32
_c_tibrv_i64                 = ctypes.c_int64
_c_tibrv_u64                 = ctypes.c_uint64
_c_tibrv_f32                 = ctypes.c_float
_c_tibrv_f64                 = ctypes.c_double
_c_tibrv_bool                = ctypes.c_int
_c_tibrv_ipport16            = ctypes.c_uint16
_c_tibrv_ipaddr32            = ctypes.c_uint32
_c_tibrv_str                 = ctypes.c_char_p

_c_tibrv_status              = ctypes.c_int32
_c_tibrvId                   = ctypes.c_uint32
_c_tibrvMsg                  = ctypes.c_void_p
_c_tibrvEvent                = _c_tibrvId
_c_tibrvDispatchable         = _c_tibrvId
_c_tibrvQueue                = _c_tibrvDispatchable
_c_tibrvQueueGroup           = _c_tibrvDispatchable
_c_tibrvTransport            = _c_tibrvId
_c_tibrvDispatcher           = _c_tibrvId
_c_tibrvEventType            = ctypes.c_uint32
_c_tibrvQueueLimitPolicy     = ctypes.c_int32
_c_tibrvIOType               = ctypes.c_int32


##-----------------------------------------------------------------------------
# TIBRV Data Types for Python
##-----------------------------------------------------------------------------
tibrv_status            = NewType('tibrv_status', int)              # int
tibrvId                 = NewType('tibrvId', int)                   # int
tibrvMsg                = NewType('tibrvMsg', int)                  # c_void_p
tibrvEvent              = NewType('tibrvEvent', int)                # tibrvId
tibrvDispatchable       = NewType('tibrvDispatchable', int)         # tibrvId
tibrvQueue              = NewType('tibrvQueue', int)                # tibrvId
tibrvQueueGroup         = NewType('tibrvQueueGroup', int)           # tibrvId
tibrvTransport          = NewType('tibrvTransport', int)            # tibrvId
tibrvDispatcher         = NewType('tibrvDispatcher', int)           # tibrvId
tibrvEventType          = NewType('tibrvEventType', int)            # enum(int)
tibrvQueueLimitPolicy   = NewType('tibrvQueueLimitPolicy', int)     # enum(int)
tibrvIOType             = NewType('tibrvIOType', int)               # enum(int)

def _cstr(sz: str, codepage = None) -> str:
    if sz is None:
        return None

    if codepage is None:
        return _c_tibrv_str(str(sz).encode())
    else:
        return _c_tibrv_str(str(sz).encode(codepage))

def _ret(param: list, val: object = None, size: int = 1) -> None:
    while len(param) < size:
        param.append(None)

    if val is None:
        param[0] = None
        return

    # assign val -> param[0]
    if val is not list:
        param[0] = val
        return

    # assign val[] -> param[]
    for i in range(len(param)):
        if i < len(val):
            param[i] = val[i]
        else:
            param[i] = None

    return

def _pystr(sz: ctypes.c_char_p, codepage = None) -> str:
    if sz is None:
        return None

    if type(sz) is bytes:
        ss = sz
    elif sz.value is None:
        return None
    else:
        ss = sz.value

    if codepage is None:
        return ss.decode()
    else:
        return ss.decode(codepage)



class RVMessage():

    # Message types
    IDENTIFY_MSG                    = 1
    DATA_MSG                        = 2
    PING_MSG                        = 3

    # Error statuses
    TIBRV_OK                        = 0
    TIBRV_INVALID_ARG               = 3
    TIBRV_INVALID_MSG               = 42


    # Types of message
    ICC_DATA_MSG_SIGNON             = 1
    ICC_DATA_MSG_SIGNOFF            = 2
    ICC_DATA_MSG_REQUEST            = 3
    ICC_DATA_MSG_REQUEST_ANSWER     = 4
    ICC_DATA_MSG_TABLE              = 5
    ICC_DATA_MSG_TABLE_ACK          = 6
    ICC_DATA_MSG_READY_ON           = 7
    ICC_DATA_MSG_READY_OFF          = 8
    ICC_DATA_MSG_RELOAD_END         = 9
    ICC_DATA_MSG_ERROR              = 10
    ICC_DATA_MSG_INFO               = 11
    ICC_DATA_MSG_TABLE_SEND         = 12
    ICC_DATA_MSG_TABLE_REQ          = 13
    ICC_DATA_MSG_EVENT              = 14


    ##-----------------------------------------------------------------------------
    # TIBRV API : tibrv/status.h
    ##-----------------------------------------------------------------------------

    _rv.tibrvStatus_GetText.argtypes = [_c_tibrv_status]
    _rv.tibrvStatus_GetText.restype = ctypes.c_char_p

    @staticmethod
    def tibrvStatus_GetText(code: tibrv_status) -> str:

        if code is None:
            return None

        c = _c_tibrv_status(code)
        sz = _rv.tibrvStatus_GetText(c)
        return _pystr(sz)

    ##-----------------------------------------------------------------------------
    # TIBRV API : tibrv/msg.h
    ##-----------------------------------------------------------------------------

    _rv.tibrvMsg_Create.argtypes = [ctypes.POINTER(_c_tibrvMsg)]
    _rv.tibrvMsg_Create.restype = _c_tibrv_status

    _rv.tibrvMsg_CreateEx.argtypes = [ctypes.POINTER(_c_tibrvMsg), _c_tibrv_u32]
    _rv.tibrvMsg_CreateEx.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_Create(initialStorage: int=0) -> (tibrv_status, tibrvMsg):

        msg = _c_tibrvMsg(0)

        if initialStorage == 0:
            status = _rv.tibrvMsg_Create(ctypes.byref(msg))
        else:
            try:
                n = _c_tibrv_u32(initialStorage)
            except:
                return RVMessage.TIBRV_INVALID_ARG, None

            status = _rv.tibrvMsg_CreateEx(ctypes.byref(msg), n)

        return status, msg.value


    _rv.tibrvMsg_SetSendSubject.argtypes = [_c_tibrvMsg, _c_tibrv_str]
    _rv.tibrvMsg_SetSendSubject.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_SetSendSubject(message: tibrvMsg, subject: str) -> tibrv_status:

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG

        try:
            sz = _cstr(subject)
        except:
            return RVMessage.TIBRV_INVALID_ARG

        status = _rv.tibrvMsg_SetSendSubject(msg, sz)

        return status


    _rv.tibrvMsg_GetSendSubject.argtypes = [_c_tibrvMsg, ctypes.POINTER(_c_tibrv_str)]
    _rv.tibrvMsg_GetSendSubject.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_GetSendSubject(message: tibrvMsg) -> (tibrv_status, str):

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG, None

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG, None

        sz = _c_tibrv_str(0)
        status = _rv.tibrvMsg_GetSendSubject(msg, ctypes.byref(sz))

        return status, _pystr(sz)

    _rv.tibrvMsg_GetReplySubject.argtypes = [_c_tibrvMsg, ctypes.POINTER(_c_tibrv_str)]
    _rv.tibrvMsg_GetReplySubject.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_GetReplySubject(message: tibrvMsg) -> (tibrv_status, str):

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG, None

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG, None

        sz = _c_tibrv_str()
        status = _rv.tibrvMsg_GetReplySubject(msg, ctypes.byref(sz))

        return status, _pystr(sz)


    _rv.tibrvMsg_ConvertToString.argtypes = [_c_tibrvMsg, ctypes.POINTER(_c_tibrv_str)]
    _rv.tibrvMsg_ConvertToString.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_ConvertToString(message: tibrvMsg, codepage: str=None) -> (tibrv_status, str):

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG, None

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG, None

        sz = _c_tibrv_str()
        status = _rv.tibrvMsg_ConvertToString(msg, ctypes.byref(sz))

        return status, _pystr(sz, codepage)

    @staticmethod
    def tibrvMsg_AddString(message: tibrvMsg, fieldName: str, value: str,
                        optIdentifier: int = 0, codepage: str = None) -> tibrv_status:

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG

        if fieldName is None or optIdentifier is None:
            return RVMessage.TIBRV_INVALID_ARG

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG

        try:
            name = _cstr(fieldName)
            val = _cstr(value, codepage)
            id = _c_tibrv_u16(optIdentifier)

        except:
            return RVMessage.TIBRV_INVALID_ARG

        status = _rv.tibrvMsg_AddStringEx(msg, name, val, id)

        return status


    _rv.tibrvMsg_AddI32Ex.argtypes = [_c_tibrvMsg, _c_tibrv_str, _c_tibrv_i32, _c_tibrv_u16]
    _rv.tibrvMsg_AddI32Ex.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_AddI32(message:tibrvMsg, fieldName:str, value:int, optIdentifier:int = 0) -> tibrv_status:

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG

        if fieldName is None or optIdentifier is None:
            return RVMessage.TIBRV_INVALID_ARG

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG

        try:
            name = _cstr(fieldName)
            val = _c_tibrv_i32(value)
            id = _c_tibrv_u16(optIdentifier)
        except:
            return RVMessage.TIBRV_INVALID_ARG

        status = _rv.tibrvMsg_AddI32Ex(msg, name, val, id)

        return status


    _rv.tibrvMsg_AddF64Ex.argtypes = [_c_tibrvMsg, _c_tibrv_str, _c_tibrv_f64, _c_tibrv_u16]
    _rv.tibrvMsg_AddF64Ex.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_AddF64(message: tibrvMsg, fieldName: str, value: float,
                        optIdentifier: int = 0) -> tibrv_status:

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG

        if fieldName is None or optIdentifier is None:
            return RVMessage.TIBRV_INVALID_ARG

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG

        try:
            name = _cstr(fieldName)
            val = _c_tibrv_f64(value)
            id = _c_tibrv_u16(optIdentifier)
        except:
            return RVMessage.TIBRV_INVALID_ARG

        status = _rv.tibrvMsg_AddF64Ex(msg, name, val, id)

        return status

    _rv.tibrvMsg_AddMsgEx.argtypes = [_c_tibrvMsg,
                                    _c_tibrv_str,
                                    _c_tibrvMsg,
                                    _c_tibrv_u16]

    _rv.tibrvMsg_AddMsgEx.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_AddMsg(message: tibrvMsg, fieldName: str, value: tibrvMsg,
                        optIdentifier: int = 0) -> tibrv_status:

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG

        if fieldName is None or optIdentifier is None:
            return RVMessage.TIBRV_INVALID_ARG

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG

        try:
            name = _cstr(fieldName)
            id = _c_tibrv_u16(optIdentifier)
            val = _c_tibrvMsg(value)
        except:
            return RVMessage.TIBRV_INVALID_ARG

        status = _rv.tibrvMsg_AddMsgEx(msg, name, val, id)
        

        return status

    _rv.tibrvMsg_GetI32Ex.argtypes = [_c_tibrvMsg,
                                    _c_tibrv_str,
                                    ctypes.POINTER(_c_tibrv_i32),
                                    _c_tibrv_u16]

    _rv.tibrvMsg_GetI32Ex.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_GetI32(message:tibrvMsg, fieldName:str, optIdentifier:int = 0) -> (tibrv_status, int):

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG, None

        if fieldName is None or optIdentifier is None:
            return RVMessage.TIBRV_INVALID_ARG, None

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG, None

        ret = None

        try:
            name = _cstr(fieldName)
            val = _c_tibrv_i32(0)
            id = _c_tibrv_u16(optIdentifier)
        except:
            return RVMessage.TIBRV_INVALID_ARG, None

        status = _rv.tibrvMsg_GetI32Ex(msg, name, ctypes.byref(val), id)

        if status == RVMessage.TIBRV_OK:
            ret = val.value

        return status, ret


    _rv.tibrvMsg_GetStringEx.argtypes = [_c_tibrvMsg,
                                        _c_tibrv_str,
                                        ctypes.POINTER(_c_tibrv_str),
                                        _c_tibrv_u16]

    _rv.tibrvMsg_GetStringEx.restype = _c_tibrv_status

    @staticmethod
    def tibrvMsg_GetString(message: tibrvMsg, fieldName: str, optIdentifier: int = 0,
                        codepage: str = None) -> (tibrv_status, str):

        if message is None or message == 0:
            return RVMessage.TIBRV_INVALID_MSG, None

        if fieldName is None or optIdentifier is None:
            return RVMessage.TIBRV_INVALID_ARG, None

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVMessage.TIBRV_INVALID_MSG, None

        ret = None

        try:
            name = _cstr(fieldName)
            val = _c_tibrv_str(0)
            id = _c_tibrv_u16(optIdentifier)
        except:
            return RVMessage.TIBRV_INVALID_ARG, None

        status = _rv.tibrvMsg_GetStringEx(msg, name, ctypes.byref(val), id)

        if status == RVMessage.TIBRV_OK:
            ret = _pystr(val, codepage)

        return status, ret

    ##########################################################


    def __init__(self, dateformat = 'DD/MM/YYYY'):
        self.subject = ""
        self.dateformat = dateformat
        status, self.message = self.tibrvMsg_Create()
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_Create', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)
    
    def __str__(self):
        return ("RVMessage object. Subject:{}".format(self.subject))

    @property
    def text(self):
        status, txt = RVMessage.tibrvMsg_ConvertToString(self.message)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_ConvertToString', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)
        return txt


    def SetSendSubject(self, send_subject: str):
        self.subject = send_subject
        status = RVMessage.tibrvMsg_SetSendSubject(self.message, send_subject)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_SetSendSubject', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)

    def GetSendSubject(self) -> str:
        status, subj_send = RVMessage.tibrvMsg_GetSendSubject(self.message)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_GetSendSubject', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)
        self.subject = subj_send
        return subj_send

    def GetReplySubject(self) -> str:
        status, subj_reply = RVMessage.tibrvMsg_GetReplySubject(self.message)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_GetReplySubject', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)
        self.reply = subj_reply
        return subj_reply

    def AddString(self, fieldName: str, value: str):
        status = RVMessage.tibrvMsg_AddString(self.message, fieldName, value)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_AddString', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)

    def AddInt(self, fieldName: str, value: int):
        status = RVMessage.tibrvMsg_AddI32(self.message, fieldName, value)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_AddInt', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)

    def AddFloat(self, fieldName: str, value: float):
        status = RVMessage.tibrvMsg_AddF64(self.message, fieldName, value)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_AddInt', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)

    def AddDateFromString(self, fieldName: str, value: str):
        status = RVMessage.tibrvMsg_AddString(self.message, fieldName, value)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_AddInt', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)

    def AddMsg(self, fieldName: str, value: tibrvMsg, optIdentifier: int = 0):
        status = RVMessage.tibrvMsg_AddMsg(self.message, fieldName, value.message)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_AddMsg', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)

    def GetString(self, fieldName: str) -> str:
        status, value = RVMessage.tibrvMsg_GetString(self.message, fieldName)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_GetString', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)
        return value

    def GetInt(self, fieldName: str) -> int:
        status, value = RVMessage.tibrvMsg_GetI32(self.message, fieldName)
        if status != RVMessage.TIBRV_OK:
            print('ERROR tibrvMsg_GetI32', status, RVMessage.TIBRVStatus_GetText(status))
            sys.exit(-1)
        return value
