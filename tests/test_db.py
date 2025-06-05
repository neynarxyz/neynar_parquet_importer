from neynar_parquet_importer.db import clean_jsonb_data


def test_clean_embeds():
    data_in = "[{'url': 'https://www.fxhash.xyz/article/interview-2023-johnwowkavic-n\\'-elout-de-kok'}]"

    data_out = clean_jsonb_data("embeds", data_in)

    assert (
        data_out[0]["url"]
        == "https://www.fxhash.xyz/article/interview-2023-johnwowkavic-n'-elout-de-kok"
    )
