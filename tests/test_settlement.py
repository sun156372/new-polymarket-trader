import unittest

from core.settlement import resolve_winner, winner_from_gamma, winner_from_ws_event


class SettlementResolutionTests(unittest.TestCase):
    def test_ws_direct_outcome_has_highest_priority(self):
        winner, source = resolve_winner(
            ws_winner="DOWN",
            gamma_payload={"closed": True, "outcomes": ["Up", "Down"], "outcomePrices": ["1", "0"]},
            twap_open=10,
            twap_close=11,
        )
        self.assertEqual((winner, source), ("DOWN", "ws_market_resolved"))

    def test_ws_event_extracts_winner_and_asset_id(self):
        self.assertEqual(
            winner_from_ws_event({"event_type": "market_resolved", "winner": "Up"}),
            "UP",
        )
        self.assertEqual(
            winner_from_ws_event(
                {"type": "market_resolved", "winning_asset_id": "token-a"},
                {"token-a": "DOWN"},
            ),
            "DOWN",
        )

    def test_gamma_requires_closed_and_resolved_prices(self):
        payload = {"closed": True, "outcomes": '["Down", "Up"]', "outcomePrices": '["1", "0"]'}
        self.assertEqual(winner_from_gamma(payload), "DOWN")
        self.assertIsNone(winner_from_gamma({**payload, "closed": False}))
        self.assertIsNone(winner_from_gamma({**payload, "outcomePrices": '["0.7", "0.3"]'}))

    def test_twap_precedes_spot_fallback(self):
        self.assertEqual(
            resolve_winner(twap_open=100, twap_close=99, spot_open=100, spot_close=101),
            ("DOWN", "chainlink_twap_fallback"),
        )

    def test_spot_is_only_used_when_prior_sources_are_unavailable(self):
        self.assertEqual(
            resolve_winner(twap_open=100, twap_close=100, spot_open=100, spot_close=101),
            ("UP", "force_local_spot_fallback"),
        )
        self.assertEqual(resolve_winner(), (None, "unresolved"))


if __name__ == "__main__":
    unittest.main()
