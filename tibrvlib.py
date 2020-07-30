import sys
import ctypes
from platform import architecture
from typing import NewType, Callable, List, Any
import time
from tibrvmsglib import RVMessage

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
tibrvcmTransport        = NewType('tibrvcmTransport', int)          # tibrvId
tibrvcmEvent            = NewType('tibrvcmEvent', int)              # tibrvId


##-----------------------------------------------------------------------------
# CALLBACK
##-----------------------------------------------------------------------------
tibrvEventCallback          = Callable[[tibrvEvent, tibrvMsg, object], None]
tibrvEventVectorCallback    = Callable[[List[tibrvMsg], int], None]
tibrvEventOnComplete        = Callable[[tibrvEvent, object], None]
tibrvQueueOnComplete        = Callable[[tibrvQueue, object], None]
tibrvQueueHook              = Callable[[tibrvQueue, object], None]
_c_tibrvEventCallback       = _func(ctypes.c_void_p, _c_tibrvEvent, _c_tibrvMsg, ctypes.c_void_p)

# keep callback/closure object from GC
# key = tibrvEvent
__callback = {}
__closure  = {}

def _reg(event, func, closure):
    __callback[event] = func
    if closure is not None:
        __closure[event] = closure

    return

def _unreg(event):
    if event in __callback:
        del __callback[event]

    if event in __closure:
        del __closure[event]

    return

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


##-----------------------------------------------------------------------------
# RVClient class
##-----------------------------------------------------------------------------


class RVClient():

    TIBRV_OK                        = 0
    TIBRV_INIT_FAILURE              = 1
    TIBRV_INVALID_TRANSPORT         = 2
    TIBRV_INVALID_ARG               = 3
    TIBRV_NOT_INITIALIZED           = 4
    TIBRV_ARG_CONFLICT              = 5

    TIBRV_SERVICE_NOT_FOUND         = 16
    TIBRV_NETWORK_NOT_FOUND         = 17
    TIBRV_DAEMON_NOT_FOUND          = 18
    TIBRV_NO_MEMORY                 = 19
    TIBRV_INVALID_SUBJECT           = 20
    TIBRV_DAEMON_NOT_CONNECTED      = 21
    TIBRV_VERSION_MISMATCH          = 22
    TIBRV_SUBJECT_COLLISION         = 23
    TIBRV_VC_NOT_CONNECTED          = 24

    TIBRV_NOT_PERMITTED             = 27

    TIBRV_INVALID_NAME              = 30
    TIBRV_INVALID_TYPE              = 31
    TIBRV_INVALID_SIZE              = 32
    TIBRV_INVALID_COUNT             = 33

    TIBRV_NOT_FOUND                 = 35
    TIBRV_ID_IN_USE                 = 36
    TIBRV_ID_CONFLICT               = 37
    TIBRV_CONVERSION_FAILED         = 38
    TIBRV_RESERVED_HANDLER          = 39
    TIBRV_ENCODER_FAILED            = 40
    TIBRV_DECODER_FAILED            = 41
    TIBRV_INVALID_MSG               = 42
    TIBRV_INVALID_FIELD             = 43
    TIBRV_INVALID_INSTANCE          = 44
    TIBRV_CORRUPT_MSG               = 45
    TIBRV_ENCODING_MISMATCH         = 46

    TIBRV_TIMEOUT                   = 50
    TIBRV_INTR                      = 51

    TIBRV_INVALID_DISPATCHABLE      = 52
    TIBRV_INVALID_DISPATCHER        = 53

    TIBRV_INVALID_EVENT             = 60
    TIBRV_INVALID_CALLBACK          = 61
    TIBRV_INVALID_QUEUE             = 62
    TIBRV_INVALID_QUEUE_GROUP       = 63

    TIBRV_INVALID_TIME_INTERVAL     = 64

    TIBRV_INVALID_IO_SOURCE         = 65
    TIBRV_INVALID_IO_CONDITION      = 66
    TIBRV_SOCKET_LIMIT              = 67

    TIBRV_OS_ERROR                  = 68

    TIBRV_INSUFFICIENT_BUFFER       = 70
    TIBRV_EOF                       = 71
    TIBRV_INVALID_FILE              = 72
    TIBRV_FILE_NOT_FOUND            = 73
    TIBRV_IO_FAILED                 = 74

    TIBRV_NOT_FILE_OWNER            = 80
    TIBRV_USERPASS_MISMATCH         = 81

    TIBRV_TOO_MANY_NEIGHBORS        = 90
    TIBRV_ALREADY_EXISTS            = 91

    TIBRV_PORT_BUSY                 = 100
    TIBRV_DELIVERY_FAILED           = 101
    TIBRV_QUEUE_LIMIT               = 102

    TIBRV_INVALID_CONTENT_DESC      = 110
    TIBRV_INVALID_SERIALIZED_BUFFER = 111
    TIBRV_DESCRIPTOR_NOT_FOUND      = 115
    TIBRV_CORRUPT_SERIALIZED_BUFFER = 116

    TIBRV_IPM_ONLY                  = 117


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
    # TIBRV API : tibrv/tibrv.h
    ##-----------------------------------------------------------------------------


    _rv.tibrv_Open.argtypes = None
    _rv.tibrv_Open.restype = _c_tibrv_status

    @staticmethod
    def tibrv_Open() -> tibrv_status:
        status = _rv.tibrv_Open()
        return status


    _rv.tibrv_Close.argtypes = None
    _rv.tibrv_Close.restype = _c_tibrv_status

    @staticmethod
    def tibrv_Close() -> tibrv_status:
        status = _rv.tibrv_Close()
        return status


    _rv.tibrv_Version.argtypes = []
    _rv.tibrv_Version.restype = ctypes.c_char_p

    @staticmethod
    def tibrv_Version() -> str:
        sz = _rv.tibrv_Version()
        return sz.decode()

    _rv.tibrvStatus_GetText.argtypes = [_c_tibrv_status]
    _rv.tibrvStatus_GetText.restype = ctypes.c_char_p

    ##-----------------------------------------------------------------------------
    # TIBRV API : tibrv/tport.h
    ##-----------------------------------------------------------------------------

    _rv.tibrvTransport_Create.argtypes = [ctypes.POINTER(_c_tibrvTransport),
                                        ctypes.c_char_p,
                                        ctypes.c_char_p,
                                        ctypes.c_char_p]
    _rv.tibrvTransport_Create.restype = _c_tibrv_status

    @staticmethod
    def tibrvTransport_Create(service: str, network: str, daemon: str) -> (tibrv_status, tibrvTransport):

        tx = _c_tibrvTransport(0)

        status = _rv.tibrvTransport_Create(ctypes.byref(tx), _cstr(service), _cstr(network), _cstr(daemon))

        return status, tx.value


    _rv.tibrvTransport_SetDescription.argtypes = [_c_tibrvTransport, ctypes.c_char_p]
    _rv.tibrvTransport_SetDescription.restype = _c_tibrv_status

    @staticmethod
    def tibrvTransport_SetDescription(transport: tibrvTransport, description: str) -> tibrv_status:

        if transport is None or transport == 0:
            return RVClient.TIBRV_INVALID_TRANSPORT

        if description is None:
            return RVClient.TIBRV_INVALID_ARG

        try:
            tx = _c_tibrvTransport(transport)
        except:
            return RVClient.TIBRV_INVALID_TRANSPORT

        try:
            sz = _cstr(description)
        except:
            return RVClient.TIBRV_INVALID_ARG

        status = _rv.tibrvTransport_SetDescription(tx, sz)

        return status


    _rv.tibrvTransport_CreateInbox.argtypes = [_c_tibrvTransport, ctypes.c_char_p, _c_tibrv_u32]
    _rv.tibrvTransport_CreateInbox.restype = _c_tibrv_status

    @staticmethod
    def tibrvTransport_CreateInbox(transport: tibrvTransport) -> (tibrv_status, str):

        if transport is None or transport == 0:
            return self.TIBRV_INVALID_TRANSPORT, None

        try:
            tx = _c_tibrvTransport(transport)
        except:
            return self.TIBRV_INVALID_TRANSPORT, None

        subj = ctypes.create_string_buffer(255) # TIBRV_SUBJECT_MAX

        status = _rv.tibrvTransport_CreateInbox(tx, subj, ctypes.sizeof(subj))

        return status, _pystr(subj)


    _rv.tibrvTransport_Destroy.argtypes = [_c_tibrvTransport]
    _rv.tibrvTransport_Destroy.restype = _c_tibrv_status

    @staticmethod
    def tibrvTransport_Destroy(transport: tibrvTransport) -> tibrv_status:

        if transport is None or transport == 0:
            return RVClient.TIBRV_INVALID_TRANSPORT

        try:
            tx = _c_tibrvTransport(transport)
        except:
            return RVClient.TIBRV_INVALID_TRANSPORT

        status = _rv.tibrvTransport_Destroy(tx)

        return status

    _rv.tibrvTransport_Send.argtypes = [_c_tibrvTransport, _c_tibrvMsg]
    _rv.tibrvTransport_Send.restype = _c_tibrv_status

    @staticmethod
    def tibrvTransport_Send(transport: tibrvTransport, message: tibrvMsg) -> tibrv_status:

        if transport is None or transport == 0:
            return RVClient.TIBRV_INVALID_TRANSPORT

        if message is None or message == 0:
            return RVClient.TIBRV_INVALID_MSG

        try:
            tx = _c_tibrvTransport(transport)
        except:
            return RVClient.TIBRV_INVALID_TRANSPORT

        try:
            msg = _c_tibrvMsg(message)
        except:
            return RVClient.TIBRV_INVALID_MSG

        status = _rv.tibrvTransport_Send(tx, msg)

        return status


    ##-----------------------------------------------------------------------------
    # TIBRV API : tibrv/queue.h
    ##-----------------------------------------------------------------------------

    _rv.tibrvQueue_Create.argtypes = [ctypes.POINTER(_c_tibrvQueue)]
    _rv.tibrvQueue_Create.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueue_Create() -> (tibrv_status, tibrvQueue):

        que = _c_tibrvQueue(0)

        status = _rv.tibrvQueue_Create(ctypes.byref(que))

        return status, que.value

    _rv.tibrvQueue_SetPriority.argtypes = [_c_tibrvQueue, _c_tibrv_u32]
    _rv.tibrvQueue_SetPriority.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueue_SetPriority(eventQueue: tibrvQueue, priority: int) -> tibrv_status:

        if eventQueue is None or eventQueue == 0:
            return RVClient.TIBRV_INVALID_QUEUE

        if priority is None:
            return RVClient.TIBRV_INVALID_ARG

        try:
            que = _c_tibrvQueue(eventQueue)
        except:
            return RVClient.TIBRV_INVALID_QUEUE

        try:
            p = _c_tibrv_u32(priority)
        except:
            return RVClient.TIBRV_INVALID_ARG

        status = _rv.tibrvQueue_SetPriority(que, p)

        return status


    _rv.tibrvQueue_DestroyEx.argtypes = [_c_tibrvQueue, ctypes.c_void_p, ctypes.c_void_p]
    _rv.tibrvQueue_DestroyEx.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueue_Destroy(eventQueue: tibrvQueue, callback : tibrvQueueOnComplete = None,
                        closure = None) -> tibrv_status:

        if eventQueue is None or eventQueue == 0:
            return RVClient.TIBRV_INVALID_QUEUE

        try:
            que = _c_tibrvQueue(eventQueue)
        except:
            return RVClient.TIBRV_INVALID_QUEUE

        if callback is None:
            cb = None
            cz = None
        else:
            try:
                cb = _c_tibrvQueueOnComplete(callback)
            except:
                return RVClient.TIBRV_INVALID_CALLBACK

            cz = ctypes.py_object(closure)

        status = _rv.tibrvQueue_DestroyEx(que, cb, cz)

        # THIS MAY CAUSE MEMORY LEAK
        # if status == TIBRV_OK and callback is not None:
        #     __reg(eventQueue, cb, closure)

        return status


    ##-----------------------------------------------------------------------------
    # TIBRV API : tibrv/ggroup.h
    ##-----------------------------------------------------------------------------


    _rv.tibrvQueueGroup_Create.argtypes = [ctypes.POINTER(_c_tibrvQueueGroup)]
    _rv.tibrvQueueGroup_Create.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueueGroup_Create() -> (tibrv_status, tibrvQueueGroup):

        grp = _c_tibrvQueue(0)

        status = _rv.tibrvQueueGroup_Create(ctypes.byref(grp))

        return status, grp.value

    # extern tibrv_status
    # tibrvQueueGroup_Add(
    #     tibrvQueueGroup             eventQueueGroup,
    #     tibrvQueue                  eventQueue);

    _rv.tibrvQueueGroup_Add.argtypes = [_c_tibrvQueueGroup, _c_tibrvQueue]
    _rv.tibrvQueueGroup_Add.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueueGroup_Add(grp: tibrvQueueGroup, queue: tibrvQueue) -> (tibrv_status):

        status = _rv.tibrvQueueGroup_Add(grp, queue)

        return status


    _rv.tibrvQueueGroup_Destroy.argtypes = [_c_tibrvQueueGroup]
    _rv.tibrvQueueGroup_Destroy.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueueGroup_Destroy(queue: tibrvQueueGroup) -> tibrv_status:

        if queue is None or queue == 0:
            return RVClient.TIBRV_INVALID_QUEUE_GROUP

        status = _rv.tibrvQueueGroup_Destroy(queue)

        return status


    _rv.tibrvQueueGroup_TimedDispatch.argtypes = [_c_tibrvQueueGroup, _c_tibrv_f64]
    _rv.tibrvQueueGroup_TimedDispatch.restype = _c_tibrv_status

    @staticmethod
    def tibrvQueueGroup_TimedDispatch(eventGroup: tibrvQueue, timeout: float) -> tibrv_status:

        if eventGroup is None or eventGroup == 0:
            return RVClient.TIBRV_INVALID_QUEUE_GROUP

        if timeout is None:
            return RVClient.TIBRV_INVALID_ARG

        try:
            que = _c_tibrvQueue(eventGroup)
        except:
            return RVClient.TIBRV_INVALID_QUEUE_GROUP

        try:
            t = _c_tibrv_f64(timeout)
        except:
            return RVClient.TIBRV_INVALID_ARG

        status = _rv.tibrvQueueGroup_TimedDispatch(que, t)

        return status


    ##-----------------------------------------------------------------------------
    # TIBRV API : tibrv/events.h
    ##-----------------------------------------------------------------------------

    _rv.tibrvEvent_CreateListener.argtypes = [ctypes.POINTER(_c_tibrvEvent),
                                            _c_tibrvQueue,
                                            _c_tibrvEventCallback,
                                            _c_tibrvTransport,
                                            _c_tibrv_str,
                                            ctypes.py_object]
    _rv.tibrvEvent_CreateListener.restype = _c_tibrv_status

    @staticmethod
    def tibrvEvent_CreateListener(queue: tibrvQueue, callback: tibrvEventCallback, transport: tibrvTransport,
                                subject: str, closure = None) -> (tibrv_status, tibrvEvent):

        if queue is None or queue == 0:
            return RVClient.TIBRV_INVALID_QUEUE, None

        if callback is None:
            return RVClient.TIBRV_INVALID_CALLBACK, None

        if transport is None or transport == 0:
            return RVClient.TIBRV_INVALID_TRANSPORT, None

        if str is None:
            return RVClient.TIBRV_INVALID_ARG, None

        ev = _c_tibrvEvent(0)

        try:
            que = _c_tibrvQueue(queue)
        except:
            return RVClient.TIBRV_INVALID_QUEUE, None

        try:
            cb = _c_tibrvEventCallback(callback)
        except:
            return RVClient.TIBRV_INVALID_CALLBACK, None

        try:
            tx = _c_tibrvTransport(transport)
        except:
            return RVClient.TIBRV_INVALID_TRANSPORT, None

        try:
            subj = _cstr(subject)
            cz = ctypes.py_object(closure)
        except:
            return RVClient.TIBRV_INVALID_ARG, None

        status = _rv.tibrvEvent_CreateListener(ctypes.byref(ev), que, cb, tx, subj, cz)

        # save cb to prevent GC
        if status == RVClient.TIBRV_OK:
            _reg(ev.value, cb, cz)

        return status, ev.value


    ##########################################################

    def __init__(self, service, network, daemon):
        self.service = service
        self.network = network
        self.daemon = daemon
        self.transport = None
        self.inbox = None
        self.connected = False

    def create(self):
        # Open connection
        print("Connect...")

        # Open TIB/RV
        status = self.tibrv_Open()
        if status != self.TIBRV_OK:
            print('ERROR tibrv_Open', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        self.version = self.tibrv_Version()
        print("Version is {}".format(self.version))


        # Create network transport

        print("Connect to daemon {}".format(self.daemon))
        status, self.transport = self.tibrvTransport_Create(self.service, self.network, self.daemon)
        if status != self.TIBRV_OK:
            print('ERROR tibrvTransport_Create', status, self.tibrvStatus_GetText(status))
            self.tibrv_Close()
            sys.exit(-1)

        # set desctiption
        self.tibrvTransport_SetDescription(self.transport, "python_transport")


        # Create two queues
        status, self.listenerQueue =  self.tibrvQueue_Create()
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueue_Create', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)
        status, self.timerQueue =  self.tibrvQueue_Create()
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueue_Create', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        # Set queues priority
        status = self.tibrvQueue_SetPriority(self.listenerQueue, 1)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueue_SetPriority', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        status = self.tibrvQueue_SetPriority(self.timerQueue, 2)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueue_SetPriority', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        # Create queue group
        status, self.queueGroup =  self.tibrvQueueGroup_Create()
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueueGroup_Create', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        # Add queues
        status = self.tibrvQueueGroup_Add(self.queueGroup, self.listenerQueue)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueueGroup_Add', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)
        status = self.tibrvQueueGroup_Add(self.queueGroup, self.timerQueue)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueueGroup_Add', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        # Create client inbox
        status, self.inbox = self.tibrvTransport_CreateInbox(self.transport)
        if status != self.TIBRV_OK:
            print('ERROR tibrvTransport_CreateInbox', status, tibrvStatus_GetText(status))
            sys.exit(-1)

        closure={}
        # Listen
        status, self.listener = self.tibrvEvent_CreateListener(self.timerQueue , self.callback, self.transport, "_RV.>", closure) 
        if status != self.TIBRV_OK:
            print('ERROR tibrvcmEvent_CreateListener', status, tibrvStatus_GetText(status))
            sys.exit(-1)

        status, self.listener = self.tibrvEvent_CreateListener(self.listenerQueue , self.callback, self.transport, self.inbox, closure) 
        if status != self.TIBRV_OK:
            print('ERROR tibrvcmEvent_CreateListener', status, tibrvStatus_GetText(status))
            sys.exit(-1)


        print("Listening on: {}".format(self.inbox))

        time.sleep(1)


    def destroy(self):
        # Close all

        if self.transport is None:
            return
        
        print("Disconnect...")

        # Destroy queue group
        status =  self.tibrvQueueGroup_Destroy(self.queueGroup)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueueGroup_Destroy', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        # Destroy two queues
        status =  self.tibrvQueue_Destroy(self.listenerQueue)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueue_Destroy', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)
        status =  self.tibrvQueue_Destroy(self.timerQueue)
        if status != self.TIBRV_OK:
            print('ERROR tibrvQueue_Destroy', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)


        if self.transport is not None:
            status = self.tibrvTransport_Destroy(self.transport)
            if status != self.TIBRV_OK:
                print('ERROR tibrvTransport_Destroy', status, self.tibrvStatus_GetText(status))
                sys.exit(-1)

        self.transport = None

        status = self.tibrv_Close()
        if status != self.TIBRV_OK:
            print('ERROR tibrv_Close', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

    def send(self, msgobj) -> bool:
        if self.transport is None:
            return False
        
        message = msgobj.message
        if message is None:
            return False

        print("Send to:", msgobj.subject)

        status = self.tibrvTransport_Send(self.transport, message)

        if status != self.TIBRV_OK:
            print('ERROR tibrvTransport_Send', status, self.tibrvStatus_GetText(status))
            sys.exit(-1)

        return True

    def connect(self, host, serv, codifier):
        self.codifier = codifier
        self.host = host
        self.serv = serv

        self.reconnect()

    def reconnect(self):

        self.destroy()
        self.inbox = ""
        self.receiver = ""
        self.create()
        self.requestConnection()

    def requestConnection(self):

        self.receiver = ""

        # Create Connection message
        msg = RVMessage()

        # Set subject into message
        msg.SetSendSubject("OKAPI.INBOX_REQUEST." + self.serv  + "." + self.host)

        # Fill the message
        msg.AddInt("Type", msg.IDENTIFY_MSG)
        msg.AddString("Inbox", self.inbox)
        msg.AddInt("Crypting", 0) # not used
        msg.AddString("Client name", self.codifier) # use codifier as name
        msg.AddString("Identity", "") # not used
        msg.AddInt("Identity type", 1) # unknown
        msg.AddInt("Okapi version", 0) # not used
        msg.AddString("Kondor+ version", "2.6.3.L2") # Must be defined !!!
        msg.AddString("Description", "") # not used
        msg.AddInt("Timeout", 60) # 1MIN ?
        msg.AddInt("Messages mode", 2) # RENDEZVOUS
        msg.AddString("Transport name", "okapi_python_transport")
        msg.AddInt("Synchronous mode", 1) # Synchronous
        msg.AddInt("Ack Allowance", 0)

        self.send(msg)

        return self.receiver

    def sendPingMessage(self):

        if self.receiver == "":
            return

        print("Ping...")

        # Create Connection message
        msg = RVMessage()

        # Set subject into message
        msg.SetSendSubject(self.receiver)
        # Fill the message
        msg.AddInt("Type", msg.PING_MSG)
        msg.AddString("Inbox", self.inbox)

        self.send(msg)

        return True

    def status(self, timeout: int):
        status = self.tibrvQueueGroup_TimedDispatch(self.queueGroup, timeout)
        return status

    def callback(self, event: tibrvcmEvent, message: tibrvMsg, closure):
        msg = RVMessage()
        msg.message = message
        subj_send = msg.GetSendSubject()
        subject = subj_send.split(".")
        # print(subject) #debug

        if subject[0] == "_RV":
            # System message
            if subject[1] in ("INFO"):
                if subject[3] == 'HOST' and subject[4] == 'STATUS':
                    #send ping after system ping
                    self.sendPingMessage()
                # skip info meggage
                return 
            if subject[1] in ("WARN"):
                # skip warn meggage
                return 
            elif subject[1] in ("ERROR"):
                self.connected = False
                self.reconnect()
            else:
                # print other message
                print("Recieve unknown:", subj_send)
                return 
        elif subject[0] == "_INBOX":
            # User message
            message_type = msg.GetInt("Type")

            if message_type == msg.IDENTIFY_MSG:
                error_type = msg.GetInt("ErrorType")
                error_message = msg.GetString("Reason")
                if error_type == 0:
                    print(error_message)
                    self.receiver = msg.GetString("Inbox")
                    return
                elif error_type == 1000:
                    print("Warning " + str(error_type), error_message)
                    self.receiver = msg.GetString("Inbox")
                    return
                elif error_type == 1001:
                    print("Error " + str(error_type), error_message + " , check import server client " + self.codifier + " in K+")
                    return
                else:
                    print("Error " + str(error_type), error_message)
                    return
            elif message_type == msg.DATA_MSG:
                print(msg.text)
            elif message_type == msg.PING_MSG:
                print("Ping answer")
                return
            else:
                print("Unknown message type", message_type)

            # if MessageType == IDENTIFY_MSG:
            #     status, error_code = tibrvMsg_GetI32(message, "ErrorType")
            #     status, error_message = tibrvMsg_GetString(message, "Reason")
            #     if status == TIBRV_OK:
            #         if error_code == ICC_ERR_SUCCESSFUL:
            #             status, self.address = tibrvMsg_GetString(message, "Inbox")
            #             self.connected = True
            #         elif error_code == ICC_ERR_ALREADY_CONNECTED:
            #             status, self.address = tibrvMsg_GetString(message, "Inbox")
            #             self.connected = True
            #         else:                
            #             print("Error {} {}".format(error_code, error_message))
            # elif MessageType == DATA_MSG:
            #     status, data_type = tibrvMsg_GetI32(message, "Data Type")
            #     if status == TIBRV_OK:
            #         if data_type == ICC_DATA_MSG_TABLE_ACK:
            #             status, self.address = tibrvMsg_GetString(message, "Inbox")
            #             self.connected = True
            #             print("Connection_Acknowledgement")
            #         elif data_type == ICC_DATA_MSG_ERROR:
            #             print("Error in message")
            #         else:
            #             pass
            #         status, NumFields = tibrvMsg_GetNumFields(message)
            #         for i in range(NumFields):
            #             status, field = tibrvMsg_GetFieldByIndex(message, i)
            #             print(field)
            #     else:
            #         print("Unknown Data Type")



        print("Recieve:", msg.GetSendSubject(), subject[0], subject[1])

