---
marp: true
---

# Nautilus Trader

--- 

## Talk Overview
- Speaker intro
- Introduction to NautilusTrader
- Features of NautilusTrader
- Using NautilusTrader
- Current state & future developments
- Live Demo

--- 

## Speaker - Chris

@Chris to fix
- Non-traditional background coming from another industry
- A long fascination and interest in financial markets, programming and computing
- Had been writing my own trading platform as what I required from existing platforms such as MetaTrader 4, C Trader, NinjaTrader didn’t exist, NautilusTrader originally written in C#
- Started working for an FX trading firm based in Singapore focusing on ML

--- 

## Speaker - Brad

- Equity Options trader/researcher/dev by profession - Ex Optiver and IMC
- Basically spend my days writing python for data science / automation of trading strategies.
- Also interested in sports betting - dabbling in tennis and basketball betting in the past with very mild success
- When I discovered Nautilus, I had been working on a similar (much more basic) system for quite a few years, but decided to drop my work and contribute to Nautilus.
- Contributing for the past year, working on Betfair adapter, better Options support and a bunch of features I thought were necessary for a production trading system.


--- 

## History of Nautilus

@Chris to fix

- The traders were using Python and so by necessity I developed what started as a thin Python client for the now distributed platform with C# data, risk and execution services
- This grew to the point that the Python codebase now heavily leveraging Cython, could stand on its own as a complete platform, which kept growing into what NautilusTrader is today
- Has been open sourced for over two years now and has reached a level of maturity where we are excited to present it to the community and hope it provides value

---

## Background
- Philosophy (correctness, performance, research → prod parity)
- Features headlines (could just briefly scroll the README)
- Event-driven, messaging
- Performance features - Cython and Rust (brief)
- Framework + System implementations (repo organization, same core system)
- High level architecture [slide]
- Strategy (brief)
- Backtest vs Live (strategy impl)

---

## What is NautilusTrader, and why?

---

## Introduction

NautilusTrader is an open-source, high-performance, production-grade algorithmic trading platform, providing quantitative traders with the ability to backtest portfolios of automated trading strategies on historical data with an event-driven engine, and also deploy those same strategies live, with no code changes.

---

## Features

The major features of the platform are:

---

**Performance**
- Event-driven backtest systems tend to have lower performance than vectorized methods
- Nautilus is written in Cython, with a Rust core currently being introduced incrementally
- The engine handles >100,000 events per second, allowing for backtesting tick data for even the most liquid products.
- Backtests are setup to be trivially parallelizable.

---

**Backtest/live parity**
- Nautilus is structured such that a huge portion of the system is unaware if it is running in backtest or live.
- The same strategy code which is developed for research and backtesting can be used to run live trading, with zero changes required.
- *Framework + System implementations (repo organization, same core system) `COPIED FROM BELOW`*

---

**Flexible**
- multiple strategies/venues/instruments in a single instance.
- Define custom external data types easily with full integration within the system.
- Pass messages between components via the message bus.

---

**Advanced**
- Time in force for orders `IOC, FOK, GTD, AT_THE_OPEN, AT_THE_CLOSE`,
- advanced order types and conditional triggers. Execution instructions post-only, reduce-only, and icebergs. Contingency order lists including `OCO, OTO`
- System clock allows scheduling events in backtest and live scenarios

---

**Extendable**;
- Integrates with any REST, WebSocket or FIX API via modular adapters.
- Capable of handling various asset classes including (but not limited to) FX, Equities, Futures, Options, CFDs, Crypto and Sports Betting - across multiple venues simultaneously.
- Extend the core system via Actors and the message bus.
- Custom data, portfolio statistics, etc.

---

# Architecture

---

@Chris to fix
- Both a framework for trading systems, as well as several system implementations for backtesting, live trading and even a sandbox environment on the way (live data with simulated execution)
- Ports and adapters architecture, which copied the architecture of the distributed C# system
- Highly modular and very well tested with a suite of over 3500 unit, integration and acceptance tests
- Loose coupling of the system components via a highly efficient message bus and single cache has allowed us to move quite quickly with changes and improvements. Components interact using various messaging patterns through the message bus (Pub/Sub, Req/Rep or point-to-point) → meaning they don’t need knowledge of, or direct references/dependencies on each other. The message bus (written in Cython) is highly performant, withy benchmarks between direct method calls or message bus being nearly identical (slightly in favor of direct calls). The loose coupling makes this worth it.

---
@Chris to fix
- Quick blurb on language choices, the right tools for the job → Python for data and non performance critical code, you can’t beat the ecosystem for iterating and prototyping fast, data and ML. Cython/Rust for performance critical components, enables the high performance even when run in a complex event-driven context.
- Introduction of the Rust core [very short headline about why Rust], and expect gradual yet continuous improvements, we’re effectively creating a framework of trading components and a trading domain model, with Python bindings over a native binary core similar to numpy and pandas which also enjoy extremely high performance.
- Core of the system identical between backtest and live, which is formalized in the code with the `NautilusKernel` (explain with diagram comparing live and backtest → essentially (literally?) exactly the same system, just different ‘adapters’)

--- 

# Complete Trading System

---

- Open source → on prem
- Backtest models (fill model, order latency)
- Orders → order tags, TIF , stop loss/take profit
- Accounts
- Positions
- Portfolio
- Persistence

---

- Actors
- Message bus
- Custom data
    - Market stats, scoreboard, twitter feed
    - Historic and live
- Risk engine
- Cache
- Clock / Time events

---

# Current State of NautilusTrader

- Multiple people using in production
- Many more testing the waters with backtesting
- Short term areas that need work / testing: (I agree to call out the areas we want to improve, its not perfect)
    - Accounting components (more testing required for margin accounts, sub accounts)
    - Configuration and deployment

---

# User Guide

---

# Getting started with NautilusTrader
- Jupyterlab docker image
    - `ghcr.io/nautechsystems/jupyterlab:develop`
- Examples directory
    - https://github.com/nautechsystems/nautilus_trader/tree/master/examples

---

## Writing a Strategy

---

### Configuration

```python
class OrderBookImbalanceConfig(StrategyConfig):
    instrument_id: str
    max_trade_size: Decimal
    trigger_min_size: float = 100.0
    trigger_imbalance_ratio: float = 0.20


class OrderBookImbalance(Strategy):
    def __init__(self, config: OrderBookImbalanceConfig):
```

---

### Start up

```python
    def on_start(self):
        """Actions to be performed on strategy start."""

        self.instrument = self.cache.instrument(self.config.instrument_id)
        
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.instrument_id}")
            self.stop()
            return

        self.subscribe_quote_ticks(instrument_id=self.instrument.id)
```

---

### Data methods

```python
def on_quote_tick(self, tick: QuoteTick):
    bid_volume = tick.bid_size
    ask_volume = tick.ask_size
    if not (bid_volume and ask_volume):
        return
    self.log.info(f"Top level: {tick.bid} @ {tick.ask}")
    smaller = min(bid_volume, ask_volume)
    larger = max(bid_volume, ask_volume)
    ratio = smaller / larger
    self.check_trigger(ratio)
```

---

### Event method

```python
def on_event(self, event: Event):
    if isinstance(event, OrderFilled):
        # Do something
        pass

```
---

# Writing an Adapter

---

## Adapters

- Convert external data or events into nautilus format
- Split into `Data` (quotes, trades, tweets) and `Execution` (order fills, account balance updates)

---

```python
class TwitterDataClient(LiveMarketDataClient):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.stream = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
        ) 

    def connect(self):
        self._loop.create_task(on_tweet())
        self._connected(True)

    async def on_tweet(self):
        for response_line in self.stream.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                tweet = Tweet(json_response)
                # Feed data to nautilus engine
                self._handle_data(data=tweet)
            await asyncio.sleep(0.1)
```

---

## Writing an Adapter

`super high level slide or two for writing an adapter`

- Twitter data