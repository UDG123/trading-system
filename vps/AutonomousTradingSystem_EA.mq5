//+------------------------------------------------------------------+
//|                                    AutonomousTradingSystem_EA.mq5 |
//|                              Institutional Autonomous Trading Sys |
//|                         Polls Railway API for approved trades     |
//+------------------------------------------------------------------+
#property copyright "Autonomous Trading System"
#property version   "3.0"
#property strict

//+------------------------------------------------------------------+
//| INPUT PARAMETERS — Set these in the EA settings                   |
//+------------------------------------------------------------------+
input string   InpRailwayURL     = "https://trading-system-production-4566.up.railway.app";
input string   InpAPIKey         = "";              // Your WEBHOOK_SECRET from Railway
input string   InpDeskFilter     = "ALL";           // Which desk: DESK1_SCALPER, DESK2_INTRADAY, etc. or ALL
input int      InpPollSeconds    = 5;               // How often to check for new trades
input int      InpTrailSeconds   = 3;               // How often to check trailing stops
input int      InpSlippage       = 30;              // Max slippage in points
input double   InpMaxDailyLoss   = 5000.0;          // Max daily loss before auto-stop
input int      InpMaxPositions   = 10;              // Max simultaneous open positions
input double   InpPartialPct     = 50.0;            // % to close at TP1 (0 = disabled)
input bool     InpMoveSLtoBE     = true;            // Move SL to breakeven at TP1
input int      InpMagicNumber    = 777777;          // EA magic number
input bool     InpLiveMode       = false;           // true = execute real trades

//+------------------------------------------------------------------+
//| GLOBAL VARIABLES                                                  |
//+------------------------------------------------------------------+
datetime g_lastPoll       = 0;
datetime g_lastTrailCheck = 0;
datetime g_lastHeartbeat  = 0;
double   g_dailyPnL       = 0;
double   g_dailyLoss      = 0;
bool     g_killSwitch     = false;
int      g_todayDate       = 0;
string   g_logPrefix       = "[ATS] ";

//--- Simulated trade tracking
struct SimTrade
{
   int      signalId;
   string   symbol;
   string   brokerSymbol;
   string   direction;
   string   deskId;
   double   entryPrice;
   double   stopLoss;
   double   currentSL;
   double   takeProfit1;
   double   takeProfit2;
   double   lotSize;
   double   highWaterMark;
   double   lowWaterMark;
   datetime openTime;
   bool     active;
   bool     tp1Hit;
   int      trailingPips;
};

SimTrade g_simTrades[];
int      g_simTradeCount = 0;

//--- Symbol mapping: system name → broker name
//--- Add/edit pairs to match your broker's symbol names
string   g_symbolMapFrom[];
string   g_symbolMapTo[];

//+------------------------------------------------------------------+
//| Expert initialization                                            |
//+------------------------------------------------------------------+
int OnInit()
{
   //--- Validate inputs
   if(InpAPIKey == "" || InpAPIKey == "YOUR-WEBHOOK-SECRET-HERE")
   {
      Alert("ERROR: Set your Railway API key in EA inputs!");
      return(INIT_PARAMETERS_INCORRECT);
   }

   if(InpRailwayURL == "")
   {
      Alert("ERROR: Set your Railway URL in EA inputs!");
      return(INIT_PARAMETERS_INCORRECT);
   }

   //--- Initialize symbol mapping
   InitSymbolMap();

   //--- Set timer for polling
   EventSetMillisecondTimer(1000);

   //--- Reset daily counters
   ResetDailyCounters();

   Print(g_logPrefix, "========================================");
   Print(g_logPrefix, "AUTONOMOUS TRADING SYSTEM EA v3.1");
   Print(g_logPrefix, "Railway: ", InpRailwayURL);
   Print(g_logPrefix, "Poll interval: ", InpPollSeconds, "s");
   Print(g_logPrefix, "Desk filter: ", InpDeskFilter);
   Print(g_logPrefix, "Live mode: ", InpLiveMode ? "YES" : "SIMULATION");
   Print(g_logPrefix, "Magic number: ", InpMagicNumber);
   Print(g_logPrefix, "========================================");

   //--- Reminder about WebRequest
   Print(g_logPrefix, "IMPORTANT: Enable WebRequest for this URL:");
   Print(g_logPrefix, "  Tools > Options > Expert Advisors");
   Print(g_logPrefix, "  Check 'Allow WebRequest for listed URL'");
   Print(g_logPrefix, "  Add: ", InpRailwayURL);

   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization                                          |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   Print(g_logPrefix, "EA stopped. Reason: ", reason);
}

//+------------------------------------------------------------------+
//| Timer event — main loop                                          |
//+------------------------------------------------------------------+
void OnTimer()
{
   //--- Check for new day
   MqlDateTime dtNow;
   TimeToStruct(TimeCurrent(), dtNow);
   int today = dtNow.day;
   if(today != g_todayDate)
   {
      ResetDailyCounters();
      g_todayDate = today;
   }

   //--- Kill switch check
   if(g_killSwitch)
   {
      return;
   }

   //--- Daily loss check
   UpdateDailyPnL();
   if(g_dailyLoss <= -InpMaxDailyLoss)
   {
      Print(g_logPrefix, "DAILY LOSS LIMIT HIT: $", DoubleToString(g_dailyLoss, 2));
      Print(g_logPrefix, "Trading halted for the day.");
      g_killSwitch = true;
      CloseAllPositions("DAILY_LIMIT");
      ReportKillSwitch("daily_loss_limit");
      return;
   }

   datetime now = TimeCurrent();

   //--- Poll for pending trades
   if(now - g_lastPoll >= InpPollSeconds)
   {
      PollPendingTrades();
      g_lastPoll = now;
   }

   //--- Check trailing stops and TP1 partial closes
   if(now - g_lastTrailCheck >= InpTrailSeconds)
   {
      ManageOpenPositions();
      MonitorSimulatedTrades();
      g_lastTrailCheck = now;
   }

   //--- Heartbeat
   if(now - g_lastHeartbeat >= 60)
   {
      SendHeartbeat();
      g_lastHeartbeat = now;
   }

   //--- Check for closed positions to report
   CheckForClosedPositions();
}

//+------------------------------------------------------------------+
//| SYMBOL MAPPING                                                   |
//+------------------------------------------------------------------+
void InitSymbolMap()
{
   //--- Add your broker's symbol names here if they differ
   //--- Format: AddSymbolMap("SYSTEM_NAME", "BROKER_NAME");
   AddSymbolMap("EURUSD", "EURUSD");
   AddSymbolMap("USDJPY", "USDJPY");
   AddSymbolMap("GBPUSD", "GBPUSD");
   AddSymbolMap("USDCHF", "USDCHF");
   AddSymbolMap("AUDUSD", "AUDUSD");
   AddSymbolMap("USDCAD", "USDCAD");
   AddSymbolMap("NZDUSD", "NZDUSD");
   AddSymbolMap("EURJPY", "EURJPY");
   AddSymbolMap("GBPJPY", "GBPJPY");
   AddSymbolMap("AUDJPY", "AUDJPY");
   AddSymbolMap("XAUUSD", "XAUUSD");
   AddSymbolMap("BTCUSD", "BTCUSD");
   AddSymbolMap("ETHUSD", "ETHUSD");
   AddSymbolMap("US30",   "US30");
   AddSymbolMap("US100",  "US100");
   AddSymbolMap("NAS100", "NAS100");
   AddSymbolMap("TSLA",   "TSLA");
}

void AddSymbolMap(string from, string to)
{
   int size = ArraySize(g_symbolMapFrom);
   ArrayResize(g_symbolMapFrom, size + 1);
   ArrayResize(g_symbolMapTo, size + 1);
   g_symbolMapFrom[size] = from;
   g_symbolMapTo[size]   = to;
}

string MapSymbol(string systemSymbol)
{
   for(int i = 0; i < ArraySize(g_symbolMapFrom); i++)
   {
      if(g_symbolMapFrom[i] == systemSymbol)
         return g_symbolMapTo[i];
   }
   return systemSymbol;  // return as-is if no mapping
}

//+------------------------------------------------------------------+
//| POLL RAILWAY FOR PENDING TRADES                                  |
//+------------------------------------------------------------------+
void PollPendingTrades()
{
   string url = InpRailwayURL + "/api/trades/pending";
   string headers = "X-API-Key: " + InpAPIKey + "\r\nContent-Type: application/json\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int timeout = 10000;

   int res = WebRequest("GET", url, headers, timeout, postData, result, resultHeaders);

   if(res == -1)
   {
      int err = GetLastError();
      if(err == 4014)
         Print(g_logPrefix, "WebRequest BLOCKED — add URL to Tools > Options > Expert Advisors");
      else
         Print(g_logPrefix, "Poll failed. Error: ", err);
      return;
   }

   if(res != 200)
   {
      Print(g_logPrefix, "Poll returned HTTP ", res);
      return;
   }

   string response = CharArrayToString(result);

   //--- Parse pending trades from JSON
   int count = ExtractInt(response, "\"count\":");
   if(count == 0)
      return;

   Print(g_logPrefix, "=== ", count, " PENDING TRADE(S) FOUND ===");

   //--- Parse each trade from the "pending" array
   string pendingArray = ExtractArray(response, "\"pending\":");
   if(pendingArray == "")
      return;

   //--- Split into individual trade objects
   string trades[];
   int numTrades = SplitJsonArray(pendingArray, trades);

   for(int i = 0; i < numTrades; i++)
   {
      ProcessPendingTrade(trades[i]);
   }
}

//+------------------------------------------------------------------+
//| PROCESS A SINGLE PENDING TRADE                                   |
//+------------------------------------------------------------------+
void ProcessPendingTrade(string &tradeJson)
{
   //--- Parse fields
   int    signalId   = ExtractInt(tradeJson, "\"signal_id\":");
   string symbol     = ExtractString(tradeJson, "\"symbol\":");
   string direction  = ExtractString(tradeJson, "\"direction\":");
   string deskId     = ExtractString(tradeJson, "\"desk_id\":");
   double price      = ExtractDouble(tradeJson, "\"price\":");
   double sl         = ExtractDouble(tradeJson, "\"stop_loss\":");
   double tp1        = ExtractDouble(tradeJson, "\"take_profit_1\":");
   double tp2        = ExtractDouble(tradeJson, "\"take_profit_2\":");
   double riskPct    = ExtractDouble(tradeJson, "\"risk_pct\":");
   string decision   = ExtractString(tradeJson, "\"claude_decision\":");

   Print(g_logPrefix, "Processing: Signal #", signalId,
         " | ", symbol, " ", direction,
         " | SL: ", sl, " | TP1: ", tp1,
         " | Risk: ", riskPct, "%",
         " | Desk: ", deskId);

   //--- Desk filter: skip trades not assigned to this EA instance
   if(InpDeskFilter != "ALL" && deskId != InpDeskFilter)
   {
      Print(g_logPrefix, "SKIP: Trade for ", deskId, " but this EA handles ", InpDeskFilter);
      return;
   }

   //--- Map to broker symbol
   string brokerSymbol = MapSymbol(symbol);

   //--- Validate symbol exists
   if(!SymbolSelect(brokerSymbol, true))
   {
      Print(g_logPrefix, "ERROR: Symbol ", brokerSymbol, " not found on broker");
      return;
   }

   //--- Check max positions
   if(CountOpenPositions() >= InpMaxPositions)
   {
      Print(g_logPrefix, "MAX POSITIONS reached (", InpMaxPositions, "). Skipping.");
      return;
   }

   //--- Calculate lot size
   double lotSize = CalculateLotSize(brokerSymbol, sl, riskPct);
   if(lotSize <= 0)
   {
      Print(g_logPrefix, "ERROR: Could not calculate valid lot size");
      return;
   }

   //--- Execute trade
   if(InpLiveMode)
   {
      ulong ticket = ExecuteTrade(brokerSymbol, direction, lotSize, sl, tp1, signalId, deskId);
      if(ticket > 0)
      {
         //--- Report execution to Railway
         ReportExecution(signalId, deskId, symbol, direction, (int)ticket,
                         GetEntryPrice(ticket), lotSize, sl, tp1);
      }
   }
   else
   {
      Print(g_logPrefix, "SIMULATION: Would execute ", direction, " ",
            DoubleToString(lotSize, 2), " lots ", brokerSymbol,
            " SL=", sl, " TP=", tp1);

      //--- Track simulated trade for result monitoring
      int idx = ArraySize(g_simTrades);
      ArrayResize(g_simTrades, idx + 1);
      g_simTrades[idx].signalId     = signalId;
      g_simTrades[idx].symbol       = symbol;
      g_simTrades[idx].brokerSymbol = brokerSymbol;
      g_simTrades[idx].direction    = direction;
      g_simTrades[idx].deskId       = deskId;
      g_simTrades[idx].entryPrice   = SymbolInfoDouble(brokerSymbol, SYMBOL_BID);
      g_simTrades[idx].stopLoss     = sl;
      g_simTrades[idx].currentSL    = sl;
      g_simTrades[idx].takeProfit1  = tp1;
      g_simTrades[idx].takeProfit2  = tp2;
      g_simTrades[idx].lotSize      = lotSize;
      g_simTrades[idx].highWaterMark = g_simTrades[idx].entryPrice;
      g_simTrades[idx].lowWaterMark  = g_simTrades[idx].entryPrice;
      g_simTrades[idx].openTime     = TimeCurrent();
      g_simTrades[idx].active       = true;
      g_simTrades[idx].tp1Hit       = false;
      g_simTrades[idx].trailingPips = 0;
      g_simTradeCount++;

      Print(g_logPrefix, "SIM TRADE #", signalId, " opened | ",
            symbol, " ", direction, " @ ", DoubleToString(g_simTrades[idx].entryPrice, 5),
            " | SL: ", sl, " | TP: ", tp1);

      //--- Report simulated execution to Railway
      ReportExecution(signalId, deskId, symbol, direction, 999000 + signalId,
                      g_simTrades[idx].entryPrice, lotSize, sl, tp1);
   }
}

//+------------------------------------------------------------------+
//| EXECUTE A TRADE ON MT5                                           |
//+------------------------------------------------------------------+
ulong ExecuteTrade(string symbol, string direction, double lots,
                   double sl, double tp, int signalId, string deskId)
{
   MqlTradeRequest request = {};
   MqlTradeResult  result  = {};

   request.action    = TRADE_ACTION_DEAL;
   request.symbol    = symbol;
   request.volume    = lots;
   request.deviation = InpSlippage;
   request.magic     = InpMagicNumber;
   request.comment   = "ATS|" + IntegerToString(signalId) + "|" + deskId;

   //--- Get current price
   double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(symbol, SYMBOL_BID);

   if(direction == "LONG")
   {
      request.type  = ORDER_TYPE_BUY;
      request.price = ask;
   }
   else if(direction == "SHORT")
   {
      request.type  = ORDER_TYPE_SELL;
      request.price = bid;
   }
   else
   {
      Print(g_logPrefix, "ERROR: Unknown direction: ", direction);
      return 0;
   }

   //--- Normalize SL/TP to symbol digits
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   request.sl = NormalizeDouble(sl, digits);
   if(tp > 0)
      request.tp = NormalizeDouble(tp, digits);

   //--- Send order
   if(!OrderSend(request, result))
   {
      Print(g_logPrefix, "ORDER FAILED | Error: ", result.retcode,
            " | ", result.comment);
      return 0;
   }

   if(result.retcode == TRADE_RETCODE_DONE || result.retcode == TRADE_RETCODE_PLACED)
   {
      Print(g_logPrefix, "ORDER FILLED | Ticket: ", result.deal,
            " | ", symbol, " ", direction,
            " | Price: ", result.price,
            " | Lots: ", lots);
      return result.deal;
   }
   else
   {
      Print(g_logPrefix, "ORDER REJECTED | Code: ", result.retcode,
            " | ", result.comment);
      return 0;
   }
}

//+------------------------------------------------------------------+
//| CALCULATE LOT SIZE FROM RISK PERCENTAGE                          |
//+------------------------------------------------------------------+
double CalculateLotSize(string symbol, double slPrice, double riskPct)
{
   if(slPrice <= 0 || riskPct <= 0)
      return 0;

   double accountBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   double riskDollars    = accountBalance * (riskPct / 100.0);

   //--- Get current price
   double currentPrice = SymbolInfoDouble(symbol, SYMBOL_ASK);
   if(currentPrice <= 0)
      return 0;

   //--- SL distance in price
   double slDistance = MathAbs(currentPrice - slPrice);
   if(slDistance <= 0)
      return 0;

   //--- Get tick value and tick size
   double tickValue = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);

   if(tickValue <= 0 || tickSize <= 0)
      return 0;

   //--- Calculate lots
   double slInTicks = slDistance / tickSize;
   double lots = riskDollars / (slInTicks * tickValue);

   //--- Apply min/max/step
   double minLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double lotStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);

   lots = MathFloor(lots / lotStep) * lotStep;
   lots = MathMax(minLot, MathMin(maxLot, lots));

   return NormalizeDouble(lots, 2);
}

//+------------------------------------------------------------------+
//| MANAGE OPEN POSITIONS — trailing, partial close, TP1 BE move     |
//+------------------------------------------------------------------+
void ManageOpenPositions()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;

      //--- Only manage our positions
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      string symbol    = PositionGetString(POSITION_SYMBOL);
      double openPrice = PositionGetDouble(POSITION_PRICE_OPEN);
      double currentSL = PositionGetDouble(POSITION_SL);
      double currentTP = PositionGetDouble(POSITION_TP);
      double volume    = PositionGetDouble(POSITION_VOLUME);
      long   posType   = PositionGetInteger(POSITION_TYPE);
      string comment   = PositionGetString(POSITION_COMMENT);

      double currentPrice;
      if(posType == POSITION_TYPE_BUY)
         currentPrice = SymbolInfoDouble(symbol, SYMBOL_BID);
      else
         currentPrice = SymbolInfoDouble(symbol, SYMBOL_ASK);

      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      double point = SymbolInfoDouble(symbol, SYMBOL_POINT);

      //--- Check if TP1 was reached (for partial close)
      if(InpPartialPct > 0 && currentTP > 0)
      {
         bool tp1Hit = false;
         if(posType == POSITION_TYPE_BUY && currentPrice >= currentTP)
            tp1Hit = true;
         if(posType == POSITION_TYPE_SELL && currentPrice <= currentTP)
            tp1Hit = true;

         if(tp1Hit && !IsPartialClosed(comment))
         {
            //--- Partial close
            double closeVolume = NormalizeDouble(volume * (InpPartialPct / 100.0),
                                 2);
            double minLot = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
            if(closeVolume >= minLot)
            {
               PartialClose(ticket, symbol, posType, closeVolume);

               //--- Move SL to breakeven
               if(InpMoveSLtoBE)
               {
                  double beSL = NormalizeDouble(openPrice, digits);
                  ModifySLTP(ticket, symbol, beSL, 0);
                  Print(g_logPrefix, "SL moved to BE: ", beSL, " on ticket ", ticket);
               }
            }
         }
      }
   }
}

//+------------------------------------------------------------------+
//| PARTIAL CLOSE A POSITION                                         |
//+------------------------------------------------------------------+
void PartialClose(ulong ticket, string symbol, long posType, double volume)
{
   MqlTradeRequest request = {};
   MqlTradeResult  result  = {};

   request.action   = TRADE_ACTION_DEAL;
   request.symbol   = symbol;
   request.volume   = volume;
   request.position = ticket;
   request.deviation = InpSlippage;
   request.magic    = InpMagicNumber;

   if(posType == POSITION_TYPE_BUY)
   {
      request.type  = ORDER_TYPE_SELL;
      request.price = SymbolInfoDouble(symbol, SYMBOL_BID);
   }
   else
   {
      request.type  = ORDER_TYPE_BUY;
      request.price = SymbolInfoDouble(symbol, SYMBOL_ASK);
   }

   if(OrderSend(request, result))
   {
      if(result.retcode == TRADE_RETCODE_DONE)
      {
         Print(g_logPrefix, "PARTIAL CLOSE | Ticket: ", ticket,
               " | Closed: ", volume, " lots | ", symbol);
      }
   }
   else
   {
      Print(g_logPrefix, "Partial close FAILED | ", result.retcode,
            " | ", result.comment);
   }
}

//+------------------------------------------------------------------+
//| MODIFY SL/TP OF A POSITION                                      |
//+------------------------------------------------------------------+
void ModifySLTP(ulong ticket, string symbol, double newSL, double newTP)
{
   MqlTradeRequest request = {};
   MqlTradeResult  result  = {};

   request.action   = TRADE_ACTION_SLTP;
   request.symbol   = symbol;
   request.position = ticket;

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

   if(newSL > 0)
      request.sl = NormalizeDouble(newSL, digits);
   else
      request.sl = PositionGetDouble(POSITION_SL);

   if(newTP > 0)
      request.tp = NormalizeDouble(newTP, digits);
   else
      request.tp = PositionGetDouble(POSITION_TP);

   if(!OrderSend(request, result))
   {
      Print(g_logPrefix, "Modify FAILED | ", result.retcode, " | ", result.comment);
   }
}

//+------------------------------------------------------------------+
//| CHECK FOR POSITIONS THAT HAVE BEEN CLOSED (SL/TP hit)           |
//+------------------------------------------------------------------+
void CheckForClosedPositions()
{
   //--- Look through recent deal history for our closed trades
   datetime dayStart = StringToTime(TimeToString(TimeCurrent(), TIME_DATE));

   if(!HistorySelect(dayStart, TimeCurrent()))
      return;

   int totalDeals = HistoryDealsTotal();

   for(int i = totalDeals - 1; i >= 0; i--)
   {
      ulong dealTicket = HistoryDealGetTicket(i);
      if(dealTicket == 0) continue;

      //--- Only our deals
      if(HistoryDealGetInteger(dealTicket, DEAL_MAGIC) != InpMagicNumber) continue;

      //--- Only closing deals (OUT)
      long entry = HistoryDealGetInteger(dealTicket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT) continue;

      //--- Check if we already reported this deal
      string dealComment = HistoryDealGetString(dealTicket, DEAL_COMMENT);
      if(StringFind(dealComment, "REPORTED") >= 0) continue;

      //--- Get deal details
      string symbol    = HistoryDealGetString(dealTicket, DEAL_SYMBOL);
      double profit    = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
      double swap      = HistoryDealGetDouble(dealTicket, DEAL_SWAP);
      double commission = HistoryDealGetDouble(dealTicket, DEAL_COMMISSION);
      double exitPrice = HistoryDealGetDouble(dealTicket, DEAL_PRICE);
      long   posId     = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);

      double totalPnl = profit + swap + commission;

      //--- Determine close reason
      string reason = DetermineCloseReason(dealTicket);

      //--- Report to Railway
      Print(g_logPrefix, "CLOSED DEAL | Ticket: ", posId,
            " | ", symbol, " | PnL: $", DoubleToString(totalPnl, 2),
            " | Reason: ", reason);

      ReportClose((int)posId, exitPrice, totalPnl, 0, reason);
   }
}

//+------------------------------------------------------------------+
//| DETERMINE WHY A TRADE WAS CLOSED                                 |
//+------------------------------------------------------------------+
string DetermineCloseReason(ulong dealTicket)
{
   long reason = HistoryDealGetInteger(dealTicket, DEAL_REASON);

   switch((int)reason)
   {
      case DEAL_REASON_SL:      return "SL";
      case DEAL_REASON_TP:      return "TP";
      case DEAL_REASON_SO:      return "STOP_OUT";
      case DEAL_REASON_CLIENT:  return "MANUAL";
      case DEAL_REASON_EXPERT:  return "EA";
      default:                  return "UNKNOWN";
   }
}

//+------------------------------------------------------------------+
//| CLOSE ALL POSITIONS — emergency kill switch                      |
//+------------------------------------------------------------------+
void CloseAllPositions(string reason)
{
   Print(g_logPrefix, "!!! CLOSING ALL POSITIONS — Reason: ", reason, " !!!");

   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      MqlTradeRequest request = {};
      MqlTradeResult  result  = {};

      request.action   = TRADE_ACTION_DEAL;
      request.position = ticket;
      request.symbol   = PositionGetString(POSITION_SYMBOL);
      request.volume   = PositionGetDouble(POSITION_VOLUME);
      request.deviation = InpSlippage * 2;
      request.magic    = InpMagicNumber;

      long posType = PositionGetInteger(POSITION_TYPE);
      if(posType == POSITION_TYPE_BUY)
      {
         request.type  = ORDER_TYPE_SELL;
         request.price = SymbolInfoDouble(request.symbol, SYMBOL_BID);
      }
      else
      {
         request.type  = ORDER_TYPE_BUY;
         request.price = SymbolInfoDouble(request.symbol, SYMBOL_ASK);
      }

      if(OrderSend(request, result))
         Print(g_logPrefix, "Closed ticket ", ticket, " | ", result.comment);
      else
         Print(g_logPrefix, "FAILED to close ticket ", ticket, " | Error: ", result.retcode);
   }
}

//+------------------------------------------------------------------+
//| REPORT EXECUTION TO RAILWAY                                      |
//+------------------------------------------------------------------+
void ReportExecution(int signalId, string deskId, string symbol,
                     string direction, int mt5Ticket, double entryPrice,
                     double lotSize, double sl, double tp)
{
   string json = "{"
      + "\"signal_id\":" + IntegerToString(signalId) + ","
      + "\"desk_id\":\"" + deskId + "\","
      + "\"symbol\":\"" + symbol + "\","
      + "\"direction\":\"" + direction + "\","
      + "\"mt5_ticket\":" + IntegerToString(mt5Ticket) + ","
      + "\"entry_price\":" + DoubleToString(entryPrice, 5) + ","
      + "\"lot_size\":" + DoubleToString(lotSize, 2) + ","
      + "\"stop_loss\":" + DoubleToString(sl, 5) + ","
      + "\"take_profit\":" + DoubleToString(tp, 5)
      + "}";

   string url = InpRailwayURL + "/api/trades/executed";
   PostToRailway(url, json, "Execution report");
}

//+------------------------------------------------------------------+
//| REPORT TRADE CLOSE TO RAILWAY                                    |
//+------------------------------------------------------------------+
//+------------------------------------------------------------------+
//| MONITOR SIMULATED TRADES — trailing SL, partial TP1, SL/TP exit  |
//+------------------------------------------------------------------+
void MonitorSimulatedTrades()
{
   if(InpLiveMode) return;  // only for simulation mode

   for(int i = ArraySize(g_simTrades) - 1; i >= 0; i--)
   {
      if(!g_simTrades[i].active) continue;

      string sym = g_simTrades[i].brokerSymbol;
      double bid = SymbolInfoDouble(sym, SYMBOL_BID);
      double ask = SymbolInfoDouble(sym, SYMBOL_ASK);
      if(bid == 0) continue;

      double entry  = g_simTrades[i].entryPrice;
      double sl     = g_simTrades[i].currentSL;
      double tp1    = g_simTrades[i].takeProfit1;
      double tp2    = g_simTrades[i].takeProfit2;
      string dir    = g_simTrades[i].direction;
      double point  = SymbolInfoDouble(sym, SYMBOL_POINT);
      if(point == 0) point = 0.00001;

      bool   isLong = (dir == "LONG" || dir == "BUY");
      double currentPrice = isLong ? bid : ask;

      //--- Update high/low water marks
      if(isLong && bid > g_simTrades[i].highWaterMark)
         g_simTrades[i].highWaterMark = bid;
      if(!isLong && ask < g_simTrades[i].lowWaterMark)
         g_simTrades[i].lowWaterMark = ask;

      //--- Check TP1 hit → move SL to breakeven
      if(!g_simTrades[i].tp1Hit && tp1 > 0)
      {
         bool tp1Reached = isLong ? (bid >= tp1) : (ask <= tp1);
         if(tp1Reached)
         {
            g_simTrades[i].tp1Hit = true;

            //--- Move SL to breakeven + small buffer
            if(InpMoveSLtoBE)
            {
               double buffer = 2 * point;
               g_simTrades[i].currentSL = isLong ? (entry + buffer) : (entry - buffer);
               Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                     " | TP1 HIT @ ", DoubleToString(tp1, 5),
                     " | SL moved to breakeven: ", DoubleToString(g_simTrades[i].currentSL, 5));
            }

            //--- Simulate partial close (log only, adjust lot size)
            if(InpPartialPct > 0)
            {
               double closedLots = NormalizeDouble(g_simTrades[i].lotSize * (InpPartialPct / 100.0), 2);
               double partialPnl = isLong ? (tp1 - entry) : (entry - tp1);
               partialPnl = partialPnl / point * closedLots * SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
               g_simTrades[i].lotSize -= closedLots;

               Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                     " | PARTIAL CLOSE ", DoubleToString(InpPartialPct, 0), "% @ TP1",
                     " | Closed: ", DoubleToString(closedLots, 2), " lots",
                     " | PnL: $", DoubleToString(partialPnl, 2),
                     " | Remaining: ", DoubleToString(g_simTrades[i].lotSize, 2), " lots");
            }

            //--- If TP2 exists, update target; otherwise trade continues with trailing
            if(tp2 > 0)
               g_simTrades[i].takeProfit1 = tp2;  // now targeting TP2

            sl = g_simTrades[i].currentSL;  // update local var
         }
      }

      //--- Trailing stop after TP1 hit
      if(g_simTrades[i].tp1Hit)
      {
         double trailDistance = MathAbs(entry - g_simTrades[i].stopLoss);  // use original SL distance
         if(trailDistance > 0)
         {
            double newTrailSL;
            if(isLong)
            {
               newTrailSL = g_simTrades[i].highWaterMark - trailDistance;
               if(newTrailSL > g_simTrades[i].currentSL)
               {
                  g_simTrades[i].currentSL = newTrailSL;
                  Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                        " | TRAILING SL → ", DoubleToString(newTrailSL, 5),
                        " (high: ", DoubleToString(g_simTrades[i].highWaterMark, 5), ")");
               }
            }
            else
            {
               newTrailSL = g_simTrades[i].lowWaterMark + trailDistance;
               if(newTrailSL < g_simTrades[i].currentSL)
               {
                  g_simTrades[i].currentSL = newTrailSL;
                  Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                        " | TRAILING SL → ", DoubleToString(newTrailSL, 5),
                        " (low: ", DoubleToString(g_simTrades[i].lowWaterMark, 5), ")");
               }
            }
            sl = g_simTrades[i].currentSL;
         }
      }

      //--- Check SL hit
      bool hitSL = false;
      if(sl > 0)
      {
         if(isLong && bid <= sl)  hitSL = true;
         if(!isLong && ask >= sl) hitSL = true;
      }

      //--- Check TP2 hit (final target)
      bool hitTP2 = false;
      if(g_simTrades[i].tp1Hit && tp2 > 0)
      {
         if(isLong && bid >= tp2)  hitTP2 = true;
         if(!isLong && ask <= tp2) hitTP2 = true;
      }

      //--- Check time-based exit (max hold duration)
      bool timeExpired = false;
      int maxHoldSeconds = 0;
      // Desk hold times: Scalper=2h, Intraday=8h, Gold=12h, Alts/Equities=24h, Swing=72h
      if(StringFind(g_simTrades[i].deskId, "SCALPER") >= 0)      maxHoldSeconds = 2 * 3600;
      else if(StringFind(g_simTrades[i].deskId, "INTRADAY") >= 0) maxHoldSeconds = 8 * 3600;
      else if(StringFind(g_simTrades[i].deskId, "GOLD") >= 0)     maxHoldSeconds = 12 * 3600;
      else if(StringFind(g_simTrades[i].deskId, "SWING") >= 0)    maxHoldSeconds = 72 * 3600;
      else maxHoldSeconds = 24 * 3600;  // Alts, Equities default

      if(maxHoldSeconds > 0)
      {
         int elapsed = (int)(TimeCurrent() - g_simTrades[i].openTime);
         if(elapsed >= maxHoldSeconds)
         {
            timeExpired = true;
            Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                  " | TIME EXIT after ", elapsed / 3600, " hours",
                  " | Desk: ", g_simTrades[i].deskId);
         }
      }

      //--- Close trade if SL, TP2, or time expired
      if(hitSL || hitTP2 || timeExpired)
      {
         double exitPrice;
         if(hitTP2)          exitPrice = tp2;
         else if(hitSL)      exitPrice = sl;
         else                exitPrice = isLong ? bid : ask;  // market price for time exit

         double pnlPips   = isLong ? (exitPrice - entry) / point : (entry - exitPrice) / point;
         double pnlDollars = pnlPips * g_simTrades[i].lotSize * SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);

         string reason;
         if(hitTP2)               reason = "TP2_HIT";
         else if(timeExpired)     reason = "TIME_EXIT";
         else if(g_simTrades[i].tp1Hit) reason = "TRAILING_SL";
         else                     reason = "SL_HIT";

         string emoji  = (pnlDollars >= 0) ? "WIN" : "LOSS";

         Print(g_logPrefix, "SIM TRADE CLOSED | #", g_simTrades[i].signalId,
               " | ", g_simTrades[i].symbol, " ", dir,
               " | Entry: ", DoubleToString(entry, 5),
               " | Exit: ", DoubleToString(exitPrice, 5),
               " | PnL: $", DoubleToString(pnlDollars, 2),
               " (", DoubleToString(pnlPips, 1), " pips)",
               " | ", emoji, " — ", reason);

         //--- Report to Railway
         ReportClose(999000 + g_simTrades[i].signalId, exitPrice,
                     pnlDollars, pnlPips, "SIM_" + reason,
                     g_simTrades[i].symbol, g_simTrades[i].deskId, dir);

         g_simTrades[i].active = false;
         g_simTradeCount--;
      }
   }
}

void ReportClose(int mt5Ticket, double exitPrice, double pnl,
                 double pnlPips, string reason,
                 string symbol = "", string deskId = "", string direction = "")
{
   string json = "{"
      + "\"mt5_ticket\":" + IntegerToString(mt5Ticket) + ","
      + "\"exit_price\":" + DoubleToString(exitPrice, 5) + ","
      + "\"pnl_dollars\":" + DoubleToString(pnl, 2) + ","
      + "\"pnl_pips\":" + DoubleToString(pnlPips, 1) + ","
      + "\"close_reason\":\"" + reason + "\"";

   if(symbol != "")
      json += ",\"symbol\":\"" + symbol + "\"";
   if(deskId != "")
      json += ",\"desk_id\":\"" + deskId + "\"";
   if(direction != "")
      json += ",\"direction\":\"" + direction + "\"";

   json += "}";

   string url = InpRailwayURL + "/api/trades/closed";
   PostToRailway(url, json, "Close report");
}

//+------------------------------------------------------------------+
//| REPORT KILL SWITCH TO RAILWAY                                    |
//+------------------------------------------------------------------+
void ReportKillSwitch(string reason)
{
   string json = "{\"scope\":\"ALL\",\"reason\":\"" + reason + "\"}";
   string url = InpRailwayURL + "/api/kill-switch?scope=ALL";
   PostToRailway(url, json, "Kill switch");
}

//+------------------------------------------------------------------+
//| SEND HEARTBEAT TO RAILWAY                                        |
//+------------------------------------------------------------------+
void SendHeartbeat()
{
   //--- Just poll health to confirm connection
   string url = InpRailwayURL + "/api/health";
   string headers = "X-API-Key: " + InpAPIKey + "\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int res = WebRequest("GET", url, headers, 5000, postData, result, resultHeaders);

   if(res == 200)
   {
      // Connection OK — silent
   }
   else
   {
      Print(g_logPrefix, "HEARTBEAT FAILED | HTTP: ", res);
   }
}

//+------------------------------------------------------------------+
//| GENERIC POST TO RAILWAY                                          |
//+------------------------------------------------------------------+
void PostToRailway(string url, string jsonBody, string description)
{
   string headers = "X-API-Key: " + InpAPIKey
                  + "\r\nContent-Type: application/json\r\n";

   char postData[];
   StringToCharArray(jsonBody, postData, 0, WHOLE_ARRAY, CP_UTF8);
   // Remove null terminator that MQL5 adds
   ArrayResize(postData, ArraySize(postData) - 1);

   char   result[];
   string resultHeaders;

   int res = WebRequest("POST", url, headers, 10000, postData, result, resultHeaders);

   if(res == 200 || res == 201)
   {
      Print(g_logPrefix, description, " sent successfully");
   }
   else
   {
      string response = CharArrayToString(result);
      Print(g_logPrefix, description, " FAILED | HTTP: ", res, " | ", response);
   }
}

//+------------------------------------------------------------------+
//| UTILITY — Count our open positions                               |
//+------------------------------------------------------------------+
int CountOpenPositions()
{
   int count = 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket > 0 && PositionGetInteger(POSITION_MAGIC) == InpMagicNumber)
         count++;
   }
   return count;
}

//+------------------------------------------------------------------+
//| UTILITY — Get entry price of a deal                              |
//+------------------------------------------------------------------+
double GetEntryPrice(ulong dealTicket)
{
   if(HistoryDealSelect(dealTicket))
      return HistoryDealGetDouble(dealTicket, DEAL_PRICE);
   return 0;
}

//+------------------------------------------------------------------+
//| UTILITY — Check if position already had partial close            |
//+------------------------------------------------------------------+
bool IsPartialClosed(string comment)
{
   return (StringFind(comment, "PARTIAL") >= 0);
}

//+------------------------------------------------------------------+
//| UTILITY — Update daily PnL tracking                              |
//+------------------------------------------------------------------+
void UpdateDailyPnL()
{
   g_dailyPnL  = 0;
   g_dailyLoss = 0;

   datetime dayStart = StringToTime(TimeToString(TimeCurrent(), TIME_DATE));
   if(!HistorySelect(dayStart, TimeCurrent()))
      return;

   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0) continue;
      if(HistoryDealGetInteger(ticket, DEAL_MAGIC) != InpMagicNumber) continue;

      long entry = HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT) continue;

      double profit = HistoryDealGetDouble(ticket, DEAL_PROFIT)
                    + HistoryDealGetDouble(ticket, DEAL_SWAP)
                    + HistoryDealGetDouble(ticket, DEAL_COMMISSION);

      g_dailyPnL += profit;
      if(profit < 0)
         g_dailyLoss += profit;
   }
}

//+------------------------------------------------------------------+
//| UTILITY — Reset daily counters                                   |
//+------------------------------------------------------------------+
void ResetDailyCounters()
{
   g_dailyPnL   = 0;
   g_dailyLoss  = 0;
   g_killSwitch = false;
   MqlDateTime dtReset;
   TimeToStruct(TimeCurrent(), dtReset);
   g_todayDate  = dtReset.day;
   Print(g_logPrefix, "Daily counters reset");
}

//+------------------------------------------------------------------+
//| JSON PARSING UTILITIES                                           |
//| MQL5 has no built-in JSON parser, so we use string extraction    |
//+------------------------------------------------------------------+

string ExtractString(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return "";

   pos += StringLen(key);

   //--- Skip whitespace
   while(pos < StringLen(json) && (StringGetCharacter(json, pos) == ' '
         || StringGetCharacter(json, pos) == '"'))
      pos++;

   //--- Find end of string value
   int endPos = StringFind(json, "\"", pos);
   if(endPos < 0) endPos = StringLen(json);

   return StringSubstr(json, pos, endPos - pos);
}

int ExtractInt(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return 0;

   pos += StringLen(key);

   //--- Skip whitespace
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;

   //--- Extract number
   string numStr = "";
   while(pos < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '-')
         numStr += ShortToString(ch);
      else
         break;
      pos++;
   }

   return (int)StringToInteger(numStr);
}

double ExtractDouble(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return 0;

   pos += StringLen(key);

   //--- Skip whitespace
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;

   //--- Handle null
   if(StringSubstr(json, pos, 4) == "null")
      return 0;

   //--- Extract number
   string numStr = "";
   while(pos < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-')
         numStr += ShortToString(ch);
      else
         break;
      pos++;
   }

   return StringToDouble(numStr);
}

string ExtractArray(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return "";

   pos += StringLen(key);

   //--- Skip whitespace
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;

   //--- Find opening bracket
   if(StringGetCharacter(json, pos) != '[')
      return "";

   //--- Find matching closing bracket
   int depth = 1;
   int start = pos + 1;
   pos++;

   while(pos < StringLen(json) && depth > 0)
   {
      ushort ch = StringGetCharacter(json, pos);
      if(ch == '[') depth++;
      if(ch == ']') depth--;
      pos++;
   }

   return StringSubstr(json, start, pos - start - 1);
}

int SplitJsonArray(string &arrayContent, string &items[])
{
   //--- Split array of JSON objects by finding matching braces
   int count = 0;
   int pos = 0;
   int len = StringLen(arrayContent);

   while(pos < len)
   {
      //--- Find opening brace
      int start = StringFind(arrayContent, "{", pos);
      if(start < 0) break;

      //--- Find matching closing brace
      int depth = 1;
      int end = start + 1;
      while(end < len && depth > 0)
      {
         ushort ch = StringGetCharacter(arrayContent, end);
         if(ch == '{') depth++;
         if(ch == '}') depth--;
         end++;
      }

      string item = StringSubstr(arrayContent, start, end - start);
      ArrayResize(items, count + 1);
      items[count] = item;
      count++;

      pos = end;
   }

   return count;
}
//+------------------------------------------------------------------+
