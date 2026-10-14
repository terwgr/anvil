import unittest

from anvil import sched


def J(i, p, a, wait=0.0, est=10.0, **kw):
    return {"id": i, "seq": int(i[1:]), "project": p, "agent": a, "order_key": int(i[1:]), "wait": wait,
            "wait0": wait, "est": est, "run_next_at": None, **kw}


def parts(**p):
    return {"p": p, "a": {}, "pl": {}, "al": {}}


class Fairness(unittest.TestCase):
    def test_least_charged_project_first(self):
        j, why = sched.pick([J("j1", "A", "x"), J("j2", "B", "y")], parts(A=100.0, B=5.0))
        self.assertEqual((j["id"], why), ("j2", "fair"))

    def test_many_submissions_earn_no_share(self):
        jobs = [J(f"j{i}", "A", "x") for i in range(1, 6)] + [J("j9", "B", "y")]
        order = [i for i, _ in sched.project_order(jobs, parts())]
        self.assertLess(order.index("j9"), 2)

    def test_agents_share_within_project(self):
        jobs = [J("j1", "A", "x"), J("j2", "A", "x"), J("j3", "A", "y")]
        order = [i for i, _ in sched.project_order(jobs, parts())]
        self.assertEqual(order, ["j1", "j3", "j2"])

    def test_overdue_precedes_fair_turn(self):
        j, why = sched.pick([J("j1", "A", "x", wait=700), J("j2", "B", "y")], parts(A=1000.0, B=0.0))
        self.assertEqual((j["id"], why), ("j1", "overdue"))

    def test_run_next_overrides(self):
        j, why = sched.pick([J("j1", "A", "x"), J("j2", "B", "y", run_next_at=1.0)], parts(B=999.0))
        self.assertEqual((j["id"], why), ("j2", "run_next"))

    def test_unknown_estimate_makes_wait_unknown(self):
        out = sched.project_order([J("j1", "A", "x", est=None), J("j2", "B", "y")], parts(), running_remaining=5)
        self.assertEqual(out, [("j1", 5), ("j2", None)])


if __name__ == "__main__":
    unittest.main()
