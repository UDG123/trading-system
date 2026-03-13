//+------------------------------------------------------------------+
//|                                    AutonomousTradingSystem_EA.mq5 |
//|                              Institutional Autonomous Trading Sys |
//|                         Polls Railway API for approved trades     |
//+------------------------------------------------------------------+
#property copyright "Autonomous Trading System"
#property version   "3.2"
#property strict

//+------------------------------------------------------------------+
//| INPUT PARAMETERS                                                 |
//+------------------------------------------------------------------+
input string   InpRailwayURL     = "https://trading-system-production-4566.up.railway.app";
input string   InpAPIKey         = "";
input string   InpDeskFilter     = "ALL";
input int      InpPollSeconds    = 5;
input int      InpTrailSeconds   = 3;
input int      InpSlippage       = 30;
input double   InpMaxDailyLoss   = 5000.0;
input int      InpMaxPositions   = 10;
input double   InpPartialPct     = 50.0;
input bool     InpMoveSLtoBE     = true;
input int      InpMagicNumber    = 777777;
input bool     InpLiveMode       = false;

//+------------------------------------------------------------------+
//| GLOBAL VARIABLES                                                  |
//+------------------------------------------------------------------+
datetime g_lastPoll       = 0;
datetime g_lastTrailCheck = 0;
datetime g_lastExitPoll   = 0;
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

//--- Symbol mapping
string   g_symbolMapFrom[];
string   g_symbolMapTo[];

//+------------------------------------------------------------------+
//| Expert initialization                                            |
//+------------------------------------------------------------------+
int OnInit()
{
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

   InitSymbolMap();
   EventSetMillisecondTimer(1000);
   ResetDailyCounters();

   Print(g_logPrefix, "========================================");
   Print(g_logPrefix, "AUTONOMOUS TRADING SYSTEM EA v3.2");
   Print(g_logPrefix, "Railway: ", InpRailwayURL);
   Print(g_logPrefix, "Poll interval: ", InpPollSeconds, "s");
   Print(g_logPrefix, "Desk filter: ", InpDeskFilter);
   Print(g_logPrefix, "Live mode: ", InpLiveMode ? "YES — REAL TRADES" : "SIMULATION");
   Print(g_logPrefix, "Magic number: ", InpMagicNumber);
   Print(g_logPrefix, "NEW: Live time exits + exit signal polling");
   Print(g_logPrefix, "========================================");
   Print(g_logPrefix, "Enable WebRequest: Tools > Options > Expert Advisors");
   Print(g_logPrefix, "Add: ", InpRailwayURL);

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
   //--- New day check
   MqlDateTime dtNow;
   TimeToStruct(TimeCurrent(), dtNow);
   int today = dtNow.day;
   if(today != g_todayDate)
   {
      ResetDailyCounters();
      g_todayDate = today;
   }

   if(g_killSwitch) return;

   //--- Daily loss check
   UpdateDailyPnL();
   if(g_dailyLoss <= -InpMaxDailyLoss)
   {
      Print(g_logPrefix, "DAILY LOSS LIMIT HIT: $", DoubleToString(g_dailyLoss, 2));
      g_killSwitch = true;
      CloseAllPositions("DAILY_LIMIT");
      ReportKillSwitch("daily_loss_limit");
      return;
   }

   datetime now = TimeCurrent();

   //--- Poll for pending entry trades
   if(now - g_lastPoll >= InpPollSeconds)
   {
      PollPendingTrades();
      g_lastPoll = now;
   }

   //--- Poll for server-side exit commands (same interval as entries)
   if(now - g_lastExitPoll >= InpPollSeconds)
   {
      PollExitCommands();
      g_lastExitPoll = now;
   }

   //--- Manage open positions: trailing, partial close, TIME EXITS
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
   AddSymbolMap("EURGBP", "EURGBP");
   AddSymbolMap("EURAUD", "EURAUD");
   AddSymbolMap("GBPAUD", "GBPAUD");
   AddSymbolMap("EURCHF", "EURCHF");
   AddSymbolMap("CADJPY", "CADJPY");
   AddSymbolMap("NZDJPY", "NZDJPY");
   AddSymbolMap("GBPCAD", "GBPCAD");
   AddSymbolMap("AUDCAD", "AUDCAD");
   AddSymbolMap("AUDNZD", "AUDNZD");
   AddSymbolMap("CHFJPY", "CHFJPY");
   AddSymbolMap("EURNZD", "EURNZD");
   AddSymbolMap("GBPNZD", "GBPNZD");
   AddSymbolMap("XAUUSD", "XAUUSD");
   AddSymbolMap("XAGUSD", "XAGUSD");
   AddSymbolMap("WTIUSD", "WTIUSD");
   AddSymbolMap("XCUUSD", "XCUUSD");
   AddSymbolMap("BTCUSD", "BTCUSD");
   AddSymbolMap("ETHUSD", "ETHUSD");
   AddSymbolMap("SOLUSD", "SOLUSD");
   AddSymbolMap("XRPUSD", "XRPUSD");
   AddSymbolMap("LINKUSD", "LINKUSD");
   AddSymbolMap("US30",   "US30");
   AddSymbolMap("US100",  "US100");
   AddSymbolMap("NAS100", "NAS100");
   AddSymbolMap("TSLA",   "TSLA");
   AddSymbolMap("AAPL",   "AAPL");
   AddSymbolMap("MSFT",   "MSFT");
   AddSymbolMap("NVDA",   "NVDA");
   AddSymbolMap("AMZN",   "AMZN");
   AddSymbolMap("META",   "META");
   AddSymbolMap("GOOGL",  "GOOGL");
   AddSymbolMap("NFLX",   "NFLX");
   AddSymbolMap("AMD",    "AMD");
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
   return systemSymbol;
}

//+------------------------------------------------------------------+
//| GET MAX HOLD SECONDS FOR A DESK                                  |
//+------------------------------------------------------------------+
int GetMaxHoldSeconds(string deskId)
{
   if(StringFind(deskId, "SCALPER") >= 0)       return 2 * 3600;
   if(StringFind(deskId, "INTRADAY") >= 0)      return 8 * 3600;
   if(StringFind(deskId, "GOLD") >= 0)           return 12 * 3600;
   if(StringFind(deskId, "SWING") >= 0)          return 72 * 3600;
   return 24 * 3600;  // Alts, Equities default
}

//+------------------------------------------------------------------+
//| EXTRACT DESK ID FROM POSITION COMMENT                            |
//+------------------------------------------------------------------+
string GetDeskFromComment(string comment)
{
   // Comment format: "ATS|signalId|DESK1_SCALPER"
   int pos1 = StringFind(comment, "|");
   if(pos1 < 0) return "";
   int pos2 = StringFind(comment, "|", pos1 + 1);
   if(pos2 < 0) return "";
   return StringSubstr(comment, pos2 + 1);
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

   int res = WebRequest("GET", url, headers, 10000, postData, result, resultHeaders);

   if(res == -1)
   {
      int err = GetLastError();
      if(err == 4014)
         Print(g_logPrefix, "WebRequest BLOCKED — add URL to Tools > Options > Expert Advisors");
      else
         Print(g_logPrefix, "Poll failed. Error: ", err);
      return;
   }

   if(res != 200) return;

   string response = CharArrayToString(result);
   int count = ExtractInt(response, "\"count\":");
   if(count == 0) return;

   Print(g_logPrefix, "=== ", count, " PENDING TRADE(S) FOUND ===");

   string pendingArray = ExtractArray(response, "\"pending\":");
   if(pendingArray == "") return;

   string trades[];
   int numTrades = SplitJsonArray(pendingArray, trades);

   for(int i = 0; i < numTrades; i++)
      ProcessPendingTrade(trades[i]);
}

//+------------------------------------------------------------------+
//| POLL FOR SERVER-SIDE EXIT COMMANDS                               |
//| Checks /api/trades/exits for trades flagged for closure by       |
//| LuxAlgo exit signals, time limits, or kill switch.               |
//+------------------------------------------------------------------+
void PollExitCommands()
{
   if(!InpLiveMode) return;  // sim trades don't need exit polling

   string deskParam = InpDeskFilter;
   string url = InpRailwayURL + "/api/trades/exits?desk=" + deskParam;
   string headers = "X-API-Key: " + InpAPIKey + "\r\nContent-Type: application/json\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int res = WebRequest("GET", url, headers, 10000, postData, result, resultHeaders);

   if(res != 200) return;

   string response = CharArrayToString(result);
   int count = ExtractInt(response, "\"count\":");
   if(count == 0) return;

   Print(g_logPrefix, "=== ", count, " EXIT COMMAND(S) RECEIVED ===");

   string exitsArray = ExtractArray(response, "\"exits\":");
   if(exitsArray == "") return;

   string exits[];
   int numExits = SplitJsonArray(exitsArray, exits);

   for(int i = 0; i < numExits; i++)
   {
      int mt5Ticket     = ExtractInt(exits[i], "\"mt5_ticket\":");
      string symbol     = ExtractString(exits[i], "\"symbol\":");
      string closeReason = ExtractString(exits[i], "\"close_reason\":");

      if(mt5Ticket <= 0) continue;

      Print(g_logPrefix, "EXIT CMD | Ticket: ", mt5Ticket,
            " | ", symbol, " | Reason: ", closeReason);

      bool closed = ClosePositionByTicket((ulong)mt5Ticket);
      if(closed)
      {
         Print(g_logPrefix, "EXIT EXECUTED | Ticket ", mt5Ticket, " closed");
      }
      else
      {
         Print(g_logPrefix, "EXIT FAILED | Ticket ", mt5Ticket,
               " — may already be closed or not found");
      }
   }
}

//+------------------------------------------------------------------+
//| CLOSE A SPECIFIC POSITION BY TICKET                              |
//+------------------------------------------------------------------+
bool ClosePositionByTicket(ulong ticket)
{
   //--- Search open positions for this ticket
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong posTicket = PositionGetTicket(i);
      if(posTicket != ticket) continue;
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      MqlTradeRequest request = {};
      MqlTradeResult  result  = {};

      request.action   = TRADE_ACTION_DEAL;
      request.position = posTicket;
      request.symbol   = PositionGetString(POSITION_SYMBOL);
      request.volume   = PositionGetDouble(POSITION_VOLUME);
      request.deviation = InpSlippage;
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
      {
         if(result.retcode == TRADE_RETCODE_DONE)
         {
            Print(g_logPrefix, "Closed ticket ", posTicket,
                  " | ", request.symbol, " | ", result.comment);
            return true;
         }
      }

      Print(g_logPrefix, "Close FAILED ticket ", posTicket,
            " | Code: ", result.retcode, " | ", result.comment);
      return false;
   }

   // Position not found in open positions
   return false;
}

//+------------------------------------------------------------------+
//| PROCESS A SINGLE PENDING TRADE                                   |
//+------------------------------------------------------------------+
void ProcessPendingTrade(string &tradeJson)
{
   int    signalId   = ExtractInt(tradeJson, "\"signal_id\":");
   string symbol     = ExtractString(tradeJson, "\"symbol\":");
   string direction  = ExtractString(tradeJson, "\"direction\":");
   string deskId     = ExtractString(tradeJson, "\"desk_id\":");
   double price      = ExtractDouble(tradeJson, "\"price\":");
   double sl         = ExtractDouble(tradeJson, "\"stop_loss\":");
   double tp1        = ExtractDouble(tradeJson, "\"take_profit_1\":");
   double tp2        = ExtractDouble(tradeJson, "\"take_profit_2\":");
   double riskPct    = ExtractDouble(tradeJson, "\"risk_pct\":");

   Print(g_logPrefix, "Processing: Signal #", signalId,
         " | ", symbol, " ", direction,
         " | SL: ", sl, " | TP1: ", tp1,
         " | Risk: ", riskPct, "%",
         " | Desk: ", deskId);

   if(InpDeskFilter != "ALL" && deskId != InpDeskFilter)
   {
      Print(g_logPrefix, "SKIP: Trade for ", deskId, " not this EA (", InpDeskFilter, ")");
      return;
   }

   string brokerSymbol = MapSymbol(symbol);

   if(!SymbolSelect(brokerSymbol, true))
   {
      Print(g_logPrefix, "ERROR: Symbol ", brokerSymbol, " not found on broker");
      return;
   }

   if(CountOpenPositions() >= InpMaxPositions)
   {
      Print(g_logPrefix, "MAX POSITIONS reached (", InpMaxPositions, "). Skipping.");
      return;
   }

   double lotSize = CalculateLotSize(brokerSymbol, sl, riskPct);
   if(lotSize <= 0)
   {
      Print(g_logPrefix, "ERROR: Invalid lot size");
      return;
   }

   if(InpLiveMode)
   {
      ulong ticket = ExecuteTrade(brokerSymbol, direction, lotSize, sl, tp1, signalId, deskId);
      if(ticket > 0)
      {
         ReportExecution(signalId, deskId, symbol, direction, (int)ticket,
                         GetEntryPrice(ticket), lotSize, sl, tp1);
      }
   }
   else
   {
      Print(g_logPrefix, "SIMULATION: Would execute ", direction, " ",
            DoubleToString(lotSize, 2), " lots ", brokerSymbol,
            " SL=", sl, " TP=", tp1);

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

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   request.sl = NormalizeDouble(sl, digits);
   if(tp > 0)
      request.tp = NormalizeDouble(tp, digits);

   if(!OrderSend(request, result))
   {
      Print(g_logPrefix, "ORDER FAILED | Error: ", result.retcode, " | ", result.comment);
      return 0;
   }

   if(result.retcode == TRADE_RETCODE_DONE || result.retcode == TRADE_RETCODE_PLACED)
   {
      Print(g_logPrefix, "ORDER FILLED | Ticket: ", result.deal,
            " | ", symbol, " ", direction, " | Price: ", result.price, " | Lots: ", lots);
      return result.deal;
   }

   Print(g_logPrefix, "ORDER REJECTED | Code: ", result.retcode, " | ", result.comment);
   return 0;
}

//+------------------------------------------------------------------+
//| CALCULATE LOT SIZE                                               |
//+------------------------------------------------------------------+
double CalculateLotSize(string symbol, double slPrice, double riskPct)
{
   if(slPrice <= 0 || riskPct <= 0) return 0;

   double accountBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   double riskDollars    = accountBalance * (riskPct / 100.0);

   double currentPrice = SymbolInfoDouble(symbol, SYMBOL_ASK);
   if(currentPrice <= 0) return 0;

   double slDistance = MathAbs(currentPrice - slPrice);
   if(slDistance <= 0) return 0;

   double tickValue = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickValue <= 0 || tickSize <= 0) return 0;

   double slInTicks = slDistance / tickSize;
   double lots = riskDollars / (slInTicks * tickValue);

   double minLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double lotStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);

   lots = MathFloor(lots / lotStep) * lotStep;
   lots = MathMax(minLot, MathMin(maxLot, lots));

   return NormalizeDouble(lots, 2);
}

//+------------------------------------------------------------------+
//| MANAGE OPEN POSITIONS                                            |
//| Handles: TP1 partial close, breakeven, AND time-based exits.     |
//| Time exits now work in BOTH live and simulation mode.            |
//+------------------------------------------------------------------+
void ManageOpenPositions()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      string symbol    = PositionGetString(POSITION_SYMBOL);
      double openPrice = PositionGetDouble(POSITION_PRICE_OPEN);
      double currentSL = PositionGetDouble(POSITION_SL);
      double currentTP = PositionGetDouble(POSITION_TP);
      double volume    = PositionGetDouble(POSITION_VOLUME);
      long   posType   = PositionGetInteger(POSITION_TYPE);
      string comment   = PositionGetString(POSITION_COMMENT);
      datetime openTime = (datetime)PositionGetInteger(POSITION_TIME);

      double currentPrice;
      if(posType == POSITION_TYPE_BUY)
         currentPrice = SymbolInfoDouble(symbol, SYMBOL_BID);
      else
         currentPrice = SymbolInfoDouble(symbol, SYMBOL_ASK);

      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

      //--- TP1 PARTIAL CLOSE + BREAKEVEN MOVE ---
      if(InpPartialPct > 0 && currentTP > 0)
      {
         bool tp1Hit = false;
         if(posType == POSITION_TYPE_BUY && currentPrice >= currentTP)
            tp1Hit = true;
         if(posType == POSITION_TYPE_SELL && currentPrice <= currentTP)
            tp1Hit = true;

         if(tp1Hit && !IsPartialClosed(comment))
         {
            double closeVolume = NormalizeDouble(volume * (InpPartialPct / 100.0), 2);
            double minLot = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
            if(closeVolume >= minLot)
            {
               PartialClose(ticket, symbol, posType, closeVolume);

               if(InpMoveSLtoBE)
               {
                  double beSL = NormalizeDouble(openPrice, digits);
                  ModifySLTP(ticket, symbol, beSL, 0);
                  Print(g_logPrefix, "SL moved to BE: ", beSL, " on ticket ", ticket);
               }
            }
         }
      }

      //--- TIME-BASED EXIT (LIVE MODE) ---
      // This was previously trapped in MonitorSimulatedTrades().
      // Now runs on REAL positions in live mode too.
      string deskId = GetDeskFromComment(comment);
      if(deskId == "" && InpDeskFilter != "ALL")
         deskId = InpDeskFilter;  // fallback to EA's desk filter

      int maxHoldSeconds = GetMaxHoldSeconds(deskId);
      if(maxHoldSeconds > 0 && openTime > 0)
      {
         int elapsed = (int)(TimeCurrent() - openTime);
         if(elapsed >= maxHoldSeconds)
         {
            Print(g_logPrefix, "TIME EXIT | Ticket: ", ticket,
                  " | ", symbol,
                  " | Held: ", elapsed / 3600, "h ", (elapsed % 3600) / 60, "m",
                  " | Max: ", maxHoldSeconds / 3600, "h",
                  " | Desk: ", deskId);

            // Close the position at market
            bool closed = ClosePositionByTicket(ticket);
            if(closed)
            {
               Print(g_logPrefix, "TIME EXIT EXECUTED | Ticket ", ticket, " closed");
               // Railway will get the close report via CheckForClosedPositions()
            }
            else
            {
               Print(g_logPrefix, "TIME EXIT FAILED | Ticket ", ticket);
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
         Print(g_logPrefix, "PARTIAL CLOSE | Ticket: ", ticket,
               " | Closed: ", volume, " lots | ", symbol);
   }
   else
   {
      Print(g_logPrefix, "Partial close FAILED | ", result.retcode, " | ", result.comment);
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

   request.sl = (newSL > 0) ? NormalizeDouble(newSL, digits) : PositionGetDouble(POSITION_SL);
   request.tp = (newTP > 0) ? NormalizeDouble(newTP, digits) : PositionGetDouble(POSITION_TP);

   if(!OrderSend(request, result))
      Print(g_logPrefix, "Modify FAILED | ", result.retcode, " | ", result.comment);
}

//+------------------------------------------------------------------+
//| CHECK FOR POSITIONS THAT HAVE BEEN CLOSED                        |
//+------------------------------------------------------------------+
void CheckForClosedPositions()
{
   datetime dayStart = StringToTime(TimeToString(TimeCurrent(), TIME_DATE));
   if(!HistorySelect(dayStart, TimeCurrent())) return;

   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong dealTicket = HistoryDealGetTicket(i);
      if(dealTicket == 0) continue;
      if(HistoryDealGetInteger(dealTicket, DEAL_MAGIC) != InpMagicNumber) continue;

      long entry = HistoryDealGetInteger(dealTicket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT) continue;

      string dealComment = HistoryDealGetString(dealTicket, DEAL_COMMENT);
      if(StringFind(dealComment, "REPORTED") >= 0) continue;

      string symbol     = HistoryDealGetString(dealTicket, DEAL_SYMBOL);
      double profit     = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
      double swap       = HistoryDealGetDouble(dealTicket, DEAL_SWAP);
      double commission = HistoryDealGetDouble(dealTicket, DEAL_COMMISSION);
      double exitPrice  = HistoryDealGetDouble(dealTicket, DEAL_PRICE);
      long   posId      = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);

      double totalPnl = profit + swap + commission;
      string reason   = DetermineCloseReason(dealTicket);

      Print(g_logPrefix, "CLOSED DEAL | Ticket: ", posId,
            " | ", symbol, " | PnL: $", DoubleToString(totalPnl, 2),
            " | Reason: ", reason);

      ReportClose((int)posId, exitPrice, totalPnl, 0, reason);
   }
}

string DetermineCloseReason(ulong dealTicket)
{
   long reason = HistoryDealGetInteger(dealTicket, DEAL_REASON);
   switch((int)reason)
   {
      case DEAL_REASON_SL:      return "SL";
      case DEAL_REASON_TP:      return "TP";
      case DEAL_REASON_SO:      return "STOP_OUT";
      case DEAL_REASON_CLIENT:  return "MANUAL";
      case DEAL_REASON_EXPERT:  return "EA_TIME_EXIT";
      default:                  return "UNKNOWN";
   }
}

//+------------------------------------------------------------------+
//| MONITOR SIMULATED TRADES (sim mode only)                         |
//+------------------------------------------------------------------+
void MonitorSimulatedTrades()
{
   if(InpLiveMode) return;

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

      // Update water marks
      if(isLong && bid > g_simTrades[i].highWaterMark)
         g_simTrades[i].highWaterMark = bid;
      if(!isLong && ask < g_simTrades[i].lowWaterMark)
         g_simTrades[i].lowWaterMark = ask;

      // TP1 hit → breakeven + partial close
      if(!g_simTrades[i].tp1Hit && tp1 > 0)
      {
         bool tp1Reached = isLong ? (bid >= tp1) : (ask <= tp1);
         if(tp1Reached)
         {
            g_simTrades[i].tp1Hit = true;

            if(InpMoveSLtoBE)
            {
               double buffer = 2 * point;
               g_simTrades[i].currentSL = isLong ? (entry + buffer) : (entry - buffer);
               Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                     " | TP1 HIT @ ", DoubleToString(tp1, 5),
                     " | SL → BE: ", DoubleToString(g_simTrades[i].currentSL, 5));
            }

            if(InpPartialPct > 0)
            {
               double closedLots = NormalizeDouble(g_simTrades[i].lotSize * (InpPartialPct / 100.0), 2);
               double partialPnl = isLong ? (tp1 - entry) : (entry - tp1);
               partialPnl = partialPnl / point * closedLots * SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
               g_simTrades[i].lotSize -= closedLots;

               Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                     " | PARTIAL CLOSE ", DoubleToString(InpPartialPct, 0), "% @ TP1",
                     " | Closed: ", DoubleToString(closedLots, 2), " lots",
                     " | PnL: $", DoubleToString(partialPnl, 2));
            }

            if(tp2 > 0)
               g_simTrades[i].takeProfit1 = tp2;

            sl = g_simTrades[i].currentSL;
         }
      }

      // Trailing stop after TP1
      if(g_simTrades[i].tp1Hit)
      {
         double trailDistance = MathAbs(entry - g_simTrades[i].stopLoss);
         if(trailDistance > 0)
         {
            double newTrailSL;
            if(isLong)
            {
               newTrailSL = g_simTrades[i].highWaterMark - trailDistance;
               if(newTrailSL > g_simTrades[i].currentSL)
                  g_simTrades[i].currentSL = newTrailSL;
            }
            else
            {
               newTrailSL = g_simTrades[i].lowWaterMark + trailDistance;
               if(newTrailSL < g_simTrades[i].currentSL)
                  g_simTrades[i].currentSL = newTrailSL;
            }
            sl = g_simTrades[i].currentSL;
         }
      }

      // Check SL
      bool hitSL = false;
      if(sl > 0)
      {
         if(isLong && bid <= sl)  hitSL = true;
         if(!isLong && ask >= sl) hitSL = true;
      }

      // Check TP2
      bool hitTP2 = false;
      if(g_simTrades[i].tp1Hit && tp2 > 0)
      {
         if(isLong && bid >= tp2)  hitTP2 = true;
         if(!isLong && ask <= tp2) hitTP2 = true;
      }

      // Time exit
      bool timeExpired = false;
      int maxHoldSeconds = GetMaxHoldSeconds(g_simTrades[i].deskId);
      if(maxHoldSeconds > 0)
      {
         int elapsed = (int)(TimeCurrent() - g_simTrades[i].openTime);
         if(elapsed >= maxHoldSeconds)
         {
            timeExpired = true;
            Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                  " | TIME EXIT after ", elapsed / 3600, "h | Desk: ", g_simTrades[i].deskId);
         }
      }

      // Close if triggered
      if(hitSL || hitTP2 || timeExpired)
      {
         double exitPrice;
         if(hitTP2)          exitPrice = tp2;
         else if(hitSL)      exitPrice = sl;
         else                exitPrice = isLong ? bid : ask;

         double pnlPips    = isLong ? (exitPrice - entry) / point : (entry - exitPrice) / point;
         double pnlDollars = pnlPips * g_simTrades[i].lotSize * SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);

         string reason;
         if(hitTP2)               reason = "TP2_HIT";
         else if(timeExpired)     reason = "TIME_EXIT";
         else if(g_simTrades[i].tp1Hit) reason = "TRAILING_SL";
         else                     reason = "SL_HIT";

         string emoji = (pnlDollars >= 0) ? "WIN" : "LOSS";

         Print(g_logPrefix, "SIM CLOSED | #", g_simTrades[i].signalId,
               " | ", g_simTrades[i].symbol, " ", dir,
               " | Entry: ", DoubleToString(entry, 5),
               " | Exit: ", DoubleToString(exitPrice, 5),
               " | PnL: $", DoubleToString(pnlDollars, 2),
               " (", DoubleToString(pnlPips, 1), "p)",
               " | ", emoji, " — ", reason);

         ReportClose(999000 + g_simTrades[i].signalId, exitPrice,
                     pnlDollars, pnlPips, "SIM_" + reason,
                     g_simTrades[i].symbol, g_simTrades[i].deskId, dir);

         g_simTrades[i].active = false;
         g_simTradeCount--;
      }
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

      ClosePositionByTicket(ticket);
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

   PostToRailway(InpRailwayURL + "/api/trades/executed", json, "Execution report");
}

//+------------------------------------------------------------------+
//| REPORT TRADE CLOSE TO RAILWAY                                    |
//+------------------------------------------------------------------+
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

   if(symbol != "")    json += ",\"symbol\":\"" + symbol + "\"";
   if(deskId != "")    json += ",\"desk_id\":\"" + deskId + "\"";
   if(direction != "") json += ",\"direction\":\"" + direction + "\"";

   json += "}";

   PostToRailway(InpRailwayURL + "/api/trades/closed", json, "Close report");
}

//+------------------------------------------------------------------+
//| REPORT KILL SWITCH TO RAILWAY                                    |
//+------------------------------------------------------------------+
void ReportKillSwitch(string reason)
{
   string json = "{\"scope\":\"ALL\",\"reason\":\"" + reason + "\"}";
   PostToRailway(InpRailwayURL + "/api/kill-switch?scope=ALL", json, "Kill switch");
}

//+------------------------------------------------------------------+
//| SEND HEARTBEAT TO RAILWAY                                        |
//+------------------------------------------------------------------+
void SendHeartbeat()
{
   string url = InpRailwayURL + "/api/health";
   string headers = "X-API-Key: " + InpAPIKey + "\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int res = WebRequest("GET", url, headers, 5000, postData, result, resultHeaders);
   if(res != 200)
      Print(g_logPrefix, "HEARTBEAT FAILED | HTTP: ", res);
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
   ArrayResize(postData, ArraySize(postData) - 1);

   char   result[];
   string resultHeaders;

   int res = WebRequest("POST", url, headers, 10000, postData, result, resultHeaders);

   if(res == 200 || res == 201)
      Print(g_logPrefix, description, " sent successfully");
   else
   {
      string response = CharArrayToString(result);
      Print(g_logPrefix, description, " FAILED | HTTP: ", res, " | ", response);
   }
}

//+------------------------------------------------------------------+
//| UTILITIES                                                        |
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

double GetEntryPrice(ulong dealTicket)
{
   if(HistoryDealSelect(dealTicket))
      return HistoryDealGetDouble(dealTicket, DEAL_PRICE);
   return 0;
}

bool IsPartialClosed(string comment)
{
   return (StringFind(comment, "PARTIAL") >= 0);
}

void UpdateDailyPnL()
{
   g_dailyPnL  = 0;
   g_dailyLoss = 0;

   datetime dayStart = StringToTime(TimeToString(TimeCurrent(), TIME_DATE));
   if(!HistorySelect(dayStart, TimeCurrent())) return;

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
      if(profit < 0) g_dailyLoss += profit;
   }
}

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
//+------------------------------------------------------------------+
string ExtractString(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return "";
   pos += StringLen(key);
   while(pos < StringLen(json) && (StringGetCharacter(json, pos) == ' '
         || StringGetCharacter(json, pos) == '"'))
      pos++;
   int endPos = StringFind(json, "\"", pos);
   if(endPos < 0) endPos = StringLen(json);
   return StringSubstr(json, pos, endPos - pos);
}

int ExtractInt(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return 0;
   pos += StringLen(key);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;
   string numStr = "";
   while(pos < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '-')
         numStr += ShortToString(ch);
      else break;
      pos++;
   }
   return (int)StringToInteger(numStr);
}

double ExtractDouble(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return 0;
   pos += StringLen(key);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;
   if(StringSubstr(json, pos, 4) == "null") return 0;
   string numStr = "";
   while(pos < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-')
         numStr += ShortToString(ch);
      else break;
      pos++;
   }
   return StringToDouble(numStr);
}

string ExtractArray(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return "";
   pos += StringLen(key);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;
   if(StringGetCharacter(json, pos) != '[') return "";
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
   int count = 0;
   int pos = 0;
   int len = StringLen(arrayContent);
   while(pos < len)
   {
      int start = StringFind(arrayContent, "{", pos);
      if(start < 0) break;
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
