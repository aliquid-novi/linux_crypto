import asyncio
import json
import time

import websockets
from pathlib import Path 

BASE_DIR = Path(__file__).resolve().parent
HEARTBEAT_PATH = BASE_DIR / "runtime" / "heartbeat.json"



SPOT_URL = "wss://ws.kraken.com/v2"
FUTURES_URL = "wss://futures.kraken.com/ws/v1"

SPOT_SYMBOL = "BTC/USD"
PERP_SYMBOL = "PF_XBTUSD"

QTY = 0.001
HOLD_HOURS = 24

# Replace with your own Kraken fee tier.
SPOT_FEE = 0.008
PERP_FEE = 0.0005

market = {
    "spot_bid": None,
    "spot_ask": None,
    "perp_bid": None,
    "perp_ask": None,
    "index": None,
    "funding_rate": 0.0,
    "funding_prediction": 0.0,
    "spot_updated_at": None,
    "futures_updated_at": None,
}

position = None


async def spot_feed():
    async with websockets.connect(SPOT_URL) as ws:
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": [SPOT_SYMBOL],
                "event_trigger": "bbo",
                "snapshot": True,
            },
        }))

        async for raw_message in ws:
            message = json.loads(raw_message)

            if message.get("channel") != "ticker":
                continue

            ticker = message["data"][0]

            market["spot_bid"] = float(ticker["bid"])
            market["spot_ask"] = float(ticker["ask"])
            market['spot_updated_at'] = time.time()


async def futures_feed():
    async with websockets.connect(FUTURES_URL) as ws:
        await ws.send(json.dumps({
            "event": "subscribe",
            "feed": "ticker",
            "product_ids": [PERP_SYMBOL],
        }))

        async for raw_message in ws:
            message = json.loads(raw_message)

            if (
                message.get("feed") != "ticker"
                or message.get("product_id") != PERP_SYMBOL
            ):
                continue

            market["perp_bid"] = float(message["bid"])
            market["perp_ask"] = float(message["ask"])
            market["index"] = float(message["index"])

            market["funding_rate"] = float(
                message.get("relative_funding_rate", 0.0)
            )

            market["funding_prediction"] = float(
                message.get(
                    "relative_funding_rate_prediction",
                    0.0,
                )
            )
            market['futures_updated_at'] = time.time()

async def heartbeat():
    while True:
        now = time.time()

        payload = {
            "timestamp": now,
            "spot_age_seconds": (
                None
                if market["spot_updated_at"] is None
                else now - market["spot_updated_at"]
            ),
            "futures_age_seconds": (
                None
                if market["futures_updated_at"] is None
                else now - market["futures_updated_at"]
            ),
            "position_open": position is not None,
        }

        temporary = HEARTBEAT_PATH.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, indent=2))
        temporary.replace(HEARTBEAT_PATH)

        await asyncio.sleep(5)


async def strategy():
    global position

    while True:
        await asyncio.sleep(1)

        if any(
            market[key] is None
            for key in [
                "spot_bid",
                "spot_ask",
                "perp_bid",
                "perp_ask",
                "index",
            ]
        ):
            continue

        now = time.time()

        if position is None:
            entry_basis = (
                market["perp_bid"] - market["spot_ask"]
            )

            expected_basis_return = (
                entry_basis / market["spot_ask"]
            )

            expected_funding_return = (
                market["funding_prediction"] * HOLD_HOURS
            )

            round_trip_fees = 2 * (SPOT_FEE + PERP_FEE)

            expected_return = (
                expected_basis_return
                + expected_funding_return
                - round_trip_fees
            )

            if (
                market["funding_prediction"] > 0
                and expected_return > 0
            ):
                position = {
                    "opened_at": now,
                    "spot_entry": market["spot_ask"],
                    "perp_entry": market["perp_bid"],
                    "funding_pnl": 0.0,
                    "last_update": now,
                }

                print(
                    "\nOPEN PAPER POSITION"
                    f"\nLong {QTY} BTC spot at "
                    f"{position['spot_entry']:.2f}"
                    f"\nShort {QTY} BTC perp at "
                    f"{position['perp_entry']:.2f}"
                    f"\nExpected return: "
                    f"{expected_return:.4%}\n"
                )

        else:
            elapsed_hours = (
                now - position["last_update"]
            ) / 3600

            # Positive funding means the short receives funding.
            position["funding_pnl"] += (
                QTY
                * market["index"]
                * market["funding_rate"]
                * elapsed_hours
            )

            position["last_update"] = now

            spot_pnl = QTY * (
                market["spot_bid"] - position["spot_entry"]
            )

            perp_pnl = QTY * (
                position["perp_entry"] - market["perp_ask"]
            )

            entry_fees = QTY * (
                position["spot_entry"] * SPOT_FEE
                + position["perp_entry"] * PERP_FEE
            )

            exit_fees = QTY * (
                market["spot_bid"] * SPOT_FEE
                + market["perp_ask"] * PERP_FEE
            )

            net_pnl = (
                spot_pnl
                + perp_pnl
                + position["funding_pnl"]
                - entry_fees
                - exit_fees
            )

            holding_time = (
                now - position["opened_at"]
            ) / 3600

            print(
                f"\rNet paper P&L: ${net_pnl:.4f} | "
                f"Funding: ${position['funding_pnl']:.4f} | "
                f"Held: {holding_time:.2f}h",
                end="",
            )

            if holding_time >= HOLD_HOURS:
                print("\nCLOSE PAPER POSITION")
                position = None


async def main():
    await asyncio.gather(
        spot_feed(),
        futures_feed(),
        strategy(),
        heartbeat()
    )


if __name__ == "__main__":
    print("Running strat...")
    asyncio.run(main())