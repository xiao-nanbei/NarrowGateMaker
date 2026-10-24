"""Original ready-history and exact frozen prediction-index bindings."""

from dataclasses import asdict

from data.observation import TradeContribution
from features.trade_book_response import ResponseState


class CurrentInformation:
    def __init__(self, profile, binding=None):
        self.response = ResponseState(max_book_age_ns=profile.max_book_age_ns,
                                      trade_coverage=profile.trade_coverage,
                                      fast_response_state=True, range_block_size=64)
        self.timeline = None
        if binding:
            from models.replay.public_input_window import GlobalPublicTimelines
            self.timeline = GlobalPublicTimelines(binding['bundle'], binding['root'])
            if (self.timeline.identity != binding['identity'] or
                    self.timeline.manifest['model_identity'] != binding['model_identity']):
                raise ValueError('frozen information identity differs')

    def observe(self, observation):
        if isinstance(observation.payload, TradeContribution):
            self.response.observe_trade(observation.ready_ns, observation.payload)
        else:
            self.response.observe_book(observation.ready_ns, observation.source_asof_ns,
                                       observation.payload, emit_frames=False)

    def query(self, now, side, original):
        import math
        self.response.advance(now)
        book_side = 'bid' if side == 'BUY' else 'ask'
        touch = self.response.frame(book_side)
        old_price = original.get('old_price')
        old = self.response.order_frame(book_side, old_price) if old_price and old_price > 0 else None
        result = dict(original_touch=asdict(touch) if touch else None,
                      original_old_price=asdict(old) if old else None,
                      frozen_prediction=None, missing=[])
        if touch is None or not touch.valid:
            result['missing'].append('original_touch_not_valid')
        index = original.get('I_current', {}).get('prediction_generation_index')
        if self.timeline is None or type(index) is not int or index < 0:
            result['missing'].append('exact_prediction_generation_unbound')
        else:
            values = self.timeline.predictions
            if index >= len(values[0]) or int(values[0][index])*10**6 > now:
                raise ValueError('future or out-of-range original prediction generation')
            def scalar(value):
                value = value.item() if hasattr(value, 'item') else value
                return value if not isinstance(value, float) or math.isfinite(value) else None
            count = self.timeline.manifest['head_count']
            result['frozen_prediction'] = dict(index=index, identity=self.timeline.identity,
                # Preserve original positional output names rather than inventing
                # a new interpretation for inactive/zero output columns.
                outputs={name: scalar(value[index]) for name, value in zip(
                    self.timeline.manifest['prediction_files'][:count], values[:count], strict=True)},
                retained_input_columns={name: scalar(value[index]) for name, value in zip(
                    self.timeline.manifest['feature_names'], values[count:], strict=True)})
        result['complete_information_support'] = 'not_certified'
        return result
