from neynar_parquet_importer.neynar_api import NeynarApiClient
from neynar_parquet_importer.settings import Settings


def test_get_portal_pricing():
    settings = Settings()

    client = NeynarApiClient.new(
        api_key=settings.neynar_api_key,
        api_url=settings.neynar_api_url,
    )

    x = client.get_portal_pricing("indexer_as_a_service")

    print(x)

    assert False
