"""Default-off continuous price choice on the existing replay order path."""
from collections import Counter

from strategy.response_action_value import ResponseActionValue


class ResponseActionRuntime:
    def __init__(self, config):
        self.model = ResponseActionValue.from_fitted(config['artifact'], config['path_id'])
        cost = config['compute']
        if (cost.get('schema') != 'response_compute.v1'
                or not cost.get('measurement_receipt')
                or cost.get('common_to_both_arms') is not True):
            raise ValueError('response action requires measured common compute contract')
        self.cost = dict(cost)
        for name in ('event_ns', 'call_ns'):
            if type(cost[name]) is not int or cost[name] <= 0:
                raise ValueError('response compute costs must be positive integer nanoseconds')
        self.counts = Counter()
        self.last_consumed = {}

    def select(self, row, *, sequence, side, order_id, old_price, target_price,
               quantity, baseline):
        if (row is None or row['call_sequence'] != sequence or row['side'] != side
                or row['order_id'] != str(order_id) or row['old_price'] != old_price
                or row['target_price'] != target_price or row['old_quantity'] != quantity):
            raise ValueError('response action lost current order/read binding')
        if self.last_consumed.get(side, -1) >= sequence:
            raise ValueError('response action decision consumed twice')
        self.last_consumed[side] = sequence
        selected, value, reason = self.model.choose(row, baseline=baseline)
        self.counts['price_calls'] += 1
        self.counts[reason] += 1
        self.counts['scored'] += int(value is not None)
        self.counts['changed_intent'] += int(selected != baseline)
        row['response_model_intent'] = dict(path_id=self.model.path_id, value=value,
            reason=reason, baseline_update=baseline, selected_update=selected,
            request_or_effect_proven=False)
        return selected

    def charge_ms(self, events):
        if events < 0:
            raise ValueError('response observation sequence moved backwards')
        ns = self.cost['call_ns'] + events * self.cost['event_ns']
        ms = (ns + 999_999) // 1_000_000
        self.counts['compute_calls'] += 1
        self.counts['maintained_events'] += events
        self.counts['charged_ns'] += ns
        self.counts['charged_ms'] += ms
        return ms

    def snapshot(self):
        return dict(path_id=self.model.path_id, compute=self.cost, counts=dict(self.counts),
                    scope='price_intent_only_see_observer_for_requests_and_execution_blocks')
