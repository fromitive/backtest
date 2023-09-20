# requirements

python 3.10 required.. (because vectorbt module needed) **TODO**


# installation

```
git clone https://github.com/fromitive/backtest.git
cd backtest
pip install -r requirements/prod.txt # production mode
pip install -r requirements/test.txt # test mode
pip install -r requirements/dev.txt # development mode
```

## execute test code

1. install test mode

```
git clone https://github.com/fromitive/backtest.git
cd backtest
pip install -r requirements/test.txt
```

2. execute pytest

```
pytest -svv
```

# Usage

## basic useage
Example for Usage in `tests` directory or `example.ipynb` file

---

## for alram and real-trading
**WARNING** don't use real api-key, if you want loss your money. just refer the file as example.
if you want to alram or trade real money using your strategy, you can refer  `cli-auto-trade.py` or `cli-discord-alram.py`

---
## the talib message

maybe, if you run the any script of the above example code, you are occured following message in below.

```
[WARNING] could not import module talib trying next..
```

if you want using talib for executing strategy just install `ta-lib` moudle.
