from fastapi import APIRouter, Request

from services.config import supported_exchanges, supported_products

router = APIRouter()


@router.get("/markets")
async def markets(request: Request) -> dict[str, object]:
    base_url = str(request.base_url).rstrip("/")
    products = supported_products()
    example_product = products[0] if products else "BTC-USD"
    return {
        "exchanges": supported_exchanges(),
        "products": products,
        "resolutions": ["1m"],
        "examples": {
            "candles": f"{base_url}/candles/{example_product}/1m?exchange=demo&limit=3",
            "crypto_websocket": f"{base_url.replace('http', 'ws', 1)}/crypto/{example_product}?exchange=demo",
            "indicators_websocket": f"{base_url.replace('http', 'ws', 1)}/indicators/{example_product}?exchange=demo",
        },
    }
