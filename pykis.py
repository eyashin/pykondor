import sys
import time
from tibrvlib import RVClient
from tibrvmsglib import RVMessage


# MAIN PROGRAM
def main(argv):
    trace_mode = 1
    serv = "kis_port"
    host = "kondor" # test1
    codifier = "RV_TEST"
    daemon =  "tcp:" + host + ":7500"
    network = ""
    service = "8888"
    dateformat = "DD/MM/YYYY"

    # create RV connection
    rv = RVClient(service, network, daemon)

	# Connect the KIS server
    rv.connect(host, serv, codifier)

    # wait for answer from KIS
    while rv.status(1) in (rv.TIBRV_OK, rv.TIBRV_TIMEOUT):
        if rv.receiver != "":
            print("Connected to KIS")
            break

    # Ping example
    # rv.sendPingMessage(kis_inbox)

    # Create test message
    msg = RVMessage()
    kis = RVMessage()
    msg.SetSendSubject(rv.receiver)

    # initialize the Rendezvous message
    msg.AddInt("Type", msg.DATA_MSG)
    msg.AddString("Inbox", rv.inbox)
    msg.AddInt("Data Type", msg.ICC_DATA_MSG_TABLE)
    msg.AddString("Key", "EquitiesDeals")

    # insert ImportTable section (essential)
    kis.AddString("Table", "ImportTable")
    kis.AddString("Action", "I")
    kis.AddString("DateFormat", dateformat)
    kis.AddString("TableName", "EquitiesDeals")

    # insert Deal section
    kis.AddString("Table", "EquitiesDeals")
    kis.AddString("DealStatus", "S" ) # Deal Status = Simulated
    kis.AddString("DealType", "B" ) # Deal Type = Buy
    kis.AddDateFromString("TradeDate", "25/01/2020") # use dateformat
    kis.AddFloat("Quantity", 12.0)
    kis.AddFloat("Price", 333.5)
    kis.AddDateFromString("SettlementDate", "27/01/2020") # use dateformat

    # insert Users reference
    kis.AddString("Table", "Users")
    kis.AddString("Users_ShortName", "KPLUS")

    # insert Folders reference
    kis.AddString("Table", "Folders")
    kis.AddString("Folders_ShortName", "TEST")

    # insert Equities reference
    kis.AddString("Table", "Equities")
    kis.AddString("Equities_ShortName", "AAPL")

    # insert Currencies reference
    kis.AddString("Table", "Currencies")
    kis.AddString("Currencies_ShortName", "USD")

    # insert ClearingModes reference
    kis.AddString("Table", "ClearingModes")
    kis.AddString("ClearingModes_ShortName", "DEFAULT")

    # assemble message
    msg.AddMsg("KPLUSFEED", kis)
    # print(msg.text)

    print("Send EquitiesDeals message")
    rv.send(msg)

    while rv.status(1) in (rv.TIBRV_OK, rv.TIBRV_TIMEOUT):
        pass # wait for answer

    rv.destroy()


if __name__ == "__main__":
    main(sys.argv)
