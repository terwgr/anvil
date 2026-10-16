"""Fair scheduling: pure functions over plain dicts so the policy is testable in isolation.

Job dict keys: id, seq, project, agent, order_key, wait (eligible seconds waited), run_next_at, est.
Participants: {"p": {project: charge}, "a": {(project, agent): charge},
               "pl": {project: last_served}, "al": {(project, agent): last_served}}
"""

UNKNOWN_EST = 300.0  # projection only; never used for limits or privileges


def _first(jobs):
    return min(jobs, key=lambda j: (j["order_key"], j["seq"]))


def _by_turn(jobs, parts):
    """The project, then the agent within it, served longest ago (never served first; ties by submission)."""
    projects = {j["project"] for j in jobs}
    p = min(projects, key=lambda p: (parts["pl"].get(p, float("-inf")),
                                     min(j["seq"] for j in jobs if j["project"] == p)))
    pj = [j for j in jobs if j["project"] == p]
    agents = {j["agent"] for j in pj}
    a = min(agents, key=lambda a: (parts["al"].get((p, a), float("-inf")),
                                   min(j["seq"] for j in pj if j["agent"] == a)))
    return p, a


def _by_charge(jobs, parts):
    """The project, then the agent within it, with the least GPU time charged (ties by submission)."""
    projects = {j["project"] for j in jobs}
    p = min(projects, key=lambda p: (parts["p"].get(p, 0.0),
                                     min(j["seq"] for j in jobs if j["project"] == p)))
    pj = [j for j in jobs if j["project"] == p]
    agents = {j["agent"] for j in pj}
    a = min(agents, key=lambda a: (parts["a"].get((p, a), 0.0),
                                   min(j["seq"] for j in pj if j["agent"] == a)))
    return p, a


def pick(jobs, parts, overdue_after=600.0, mode="time"):
    """Return (job, reason) for the next start, or (None, None).
    mode "time": equal GPU time per project, then per agent. "turns": strict round robin, one job per turn."""
    if not jobs:
        return None, None
    rn = [j for j in jobs if j.get("run_next_at")]
    if not rn:
        normal = [j for j in jobs if not j.get("last_at")]
        if not normal:  # only jobs moved to the back by hand: in the order they were moved
            return min(jobs, key=lambda j: (j["last_at"], j["seq"])), "last"
        jobs = normal
    if rn:
        return min(rn, key=lambda j: (j["run_next_at"], j["seq"])), "run_next"

    overdue = [j for j in jobs if j["wait"] >= overdue_after]
    if overdue:
        # overdue narrows who may go next; among them the usual rule decides (with long jobs nearly everything
        # becomes overdue, and picking by turn there would hand out GPU time by job length)
        p, a = (_by_turn if mode == "turns" else _by_charge)(overdue, parts)
        # the agent's next job in its own order (all its eligible jobs, not only the overdue one)
        return _first([j for j in jobs if j["project"] == p and j["agent"] == a]), "overdue"

    if mode == "turns":
        p, a = _by_turn(jobs, parts)
        return _first([j for j in jobs if j["project"] == p and j["agent"] == a]), "turn"

    p, a = _by_charge(jobs, parts)
    return _first([j for j in jobs if j["project"] == p and j["agent"] == a]), "fair"


def project_order(jobs, parts, overdue_after=600.0, running_remaining=0.0, mode="time"):
    """Simulate future picks to publish a global order. Returns [(job_id, est_wait_or_None)].

    Projection charges each simulated job its estimate (UNKNOWN_EST if unknown) purely to order
    the remaining jobs; the wait estimate becomes None once any unknown duration is ahead.
    """
    jobs = [dict(j) for j in jobs]
    parts = {k: dict(v) for k, v in parts.items()}
    out = []
    t = running_remaining if running_remaining is not None else 0.0
    known = running_remaining is not None
    clock = 0.0
    while jobs:
        for j in jobs:
            j["wait"] = j["wait0"] + clock
        j, _ = pick(jobs, parts, overdue_after, mode)
        out.append((j["id"], round(t) if known else None))
        est = j["est"]
        cost = est if est is not None else UNKNOWN_EST
        if est is None:
            known = False
        else:
            t += est
        clock += cost
        key = (j["project"], j["agent"])
        parts["p"][j["project"]] = parts["p"].get(j["project"], 0.0) + cost
        parts["a"][key] = parts["a"].get(key, 0.0) + cost
        parts["pl"][j["project"]] = 1e18 + clock
        parts["al"][key] = 1e18 + clock
        jobs = [x for x in jobs if x["id"] != j["id"]]
    return out
