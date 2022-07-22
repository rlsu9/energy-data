#!/usr/bin/env python3

def test_balancing_authority_list(client, logger):
    response = client.get('/balancing-authority/list')
    assert response.status_code < 400, "status_code not ok: %d" % response.status_code

    expected_regions_subset = {
        'US-BPA',
        'US-CAISO',
    }
    actual_regions: list[str] = response.json
    logger.debug("Received %d regions." % len(actual_regions))
    assert expected_regions_subset.issubset(actual_regions)
