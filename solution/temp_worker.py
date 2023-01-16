from worker.abstract_worker import Worker


class TempWorker(Worker):
    def do_work(self, info: dict, attr: dict) -> dict:
        result = 2 // info.get("a")
        self.logger.info(result)
        return dict(msg=result, test="success")


if __name__ == '__main__':
    x = TempWorker(__file__)
    b = 1
    test_set = dict(
        slack_channel="ap_james_test",
        error_slack_channel="ap_james_test",
        a=b,
    )
    test_attr = dict()
    x.work(test_set, test_attr)
