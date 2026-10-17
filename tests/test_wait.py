import os
import sqlite3
import tempfile
import time
import unittest
from unittest import mock

from anvil import service


class Waits(unittest.TestCase):
    """Receipts and confirmed delivery against a real service (scheduler, runner) in a throwaway ANVIL_HOME."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.env = mock.patch.dict(os.environ, {"ANVIL_HOME": cls.tmp.name})
        cls.env.start()
        cls.svc = service.Service()
        cls.svc.start()

    @classmethod
    def tearDownClass(cls):
        cls.svc.stopping = True
        cls.env.stop()
        cls.tmp.cleanup()

    def submit(self, agent, script):
        return self.svc.submit({"project": "t", "agent": agent, "name": "sh", "cmd": ["/bin/sh", "-c", script],
                                "cwd": self.tmp.name, "check_drift": False})["id"]

    def drain(self, agent, confirm=False):
        """Every job an agent-wide wait hands out (acknowledging each answer), until it says none_pending."""
        got = []
        while True:
            r = self.svc.wait(agent=agent, until_done=True, timeout=10, confirm=confirm)
            self.assertFalse(r.get("unchanged"), r)
            if r.get("none_pending"):
                return got
            got += r["jobs"]
            if confirm:
                self.svc.ack(r["delivery"])

    def test_failure_receipt_has_the_log_tail(self):
        jid = self.submit("tail", "printf 'step 1\\rstep 2\\n'; printf '\\033[31mboom\\033[0m\\n'; exit 3")
        r = self.svc.wait(jid, until_done=True, timeout=10)
        self.assertEqual((r["status"], r["exit_code"]), ("failed", 3))
        self.assertEqual(r["log_tail"], "step 2\nboom")

    def test_result_files_are_listed(self):
        jid = self.submit("files", "touch \"$ANVIL_RESULT_DIR/metrics.json\"")
        r = self.svc.wait(jid, until_done=True, timeout=10)
        self.assertEqual(r["results"], ["metrics.json"])

    def test_unacknowledged_results_are_sent_again(self):
        jid = self.submit("lost", "true")
        r = self.svc.wait(agent="lost", until_done=True, timeout=10, confirm=True)
        self.assertEqual([j["id"] for j in r["jobs"]], [jid])
        # leased: not handed out twice at once, and not none_pending either (it may never have arrived)
        self.assertTrue(self.svc.wait(agent="lost", until_done=True, timeout=0.2, confirm=True).get("unchanged"))
        with mock.patch.object(service, "LEASE_S", 0):
            again = self.svc.wait(agent="lost", until_done=True, timeout=10, confirm=True)
        self.assertEqual([j["id"] for j in again["jobs"]], [jid])
        self.assertEqual(self.svc.ack(again["delivery"]), {"acked": 1})
        self.assertEqual(self.svc.ack(r["delivery"]), {"acked": 0})  # superseded by the second delivery
        self.assertEqual(self.drain("lost", confirm=True), [])

    def test_acknowledged_results_are_delivered_once(self):
        ids = [self.submit("once", "true"), self.submit("once", "exit 1")]
        self.assertEqual(sorted(j["id"] for j in self.drain("once", confirm=True)), sorted(ids))

    def test_older_clients_count_as_delivered_at_once(self):
        jid = self.submit("legacy", "true")
        self.assertEqual([j["id"] for j in self.drain("legacy")], [jid])

    def test_released_hold_is_reported_again(self):
        self.svc.set_paused(True, "human")  # so it can't start before the hold
        jid = self.submit("hold", "true")
        self.svc.control(jid, "hold", "hold")
        self.svc.set_paused(False, "human")
        r =self.svc.wait(agent="hold", until_done=True, timeout=10, confirm=True)
        self.assertEqual(r["action_required"], [jid])
        self.svc.ack(r["delivery"])
        self.svc.control(jid, "release", "hold")
        self.assertEqual([j["status"] for j in self.drain("hold")], ["succeeded"])


class Upgrade(unittest.TestCase):
    def test_version_3_database_gains_delivery_columns(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "anvil.db")
            service.open_db(p).close()
            db = sqlite3.connect(p)
            db.execute("ALTER TABLE jobs DROP COLUMN delivery")
            db.execute("ALTER TABLE jobs DROP COLUMN sent_at")
            db.execute("PRAGMA user_version=3")
            db.commit()
            db.close()
            cols = {r[1] for r in service.open_db(p).execute("PRAGMA table_info(jobs)")}
            self.assertLessEqual({"delivery", "sent_at"}, cols)


if __name__ == "__main__":
    unittest.main()
