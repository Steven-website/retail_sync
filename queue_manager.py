"""
Sistema de cola para operaciones de escritura concurrentes.
Previene pérdida de datos cuando múltiples usuarios modifican BASE.parquet al mismo tiempo.
"""
import json
import uuid
import time
import base64
import os
import requests
import streamlit as st
from datetime import datetime, timezone

QUEUE_FILE        = "_queue.json"
LOCK_TIMEOUT_SECS = 300   # 5 min → lock activo se auto-expira
WAIT_TIMEOUT_SECS = 600   # 10 min → entradas en espera se expiran

# ─── GITHUB I/O ───────────────────────────────────────────

def _headers():
    token = st.secrets.get("GITHUB_TOKEN", os.environ.get("GITHUB_TOKEN", ""))
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

def _repo():
    return st.secrets.get("GITHUB_REPO", os.environ.get("GITHUB_REPO", ""))

def _branch():
    return st.secrets.get("GITHUB_BRANCH", os.environ.get("GITHUB_BRANCH", "main"))

def _now():
    return datetime.now(timezone.utc).isoformat()

def _elapsed_secs(iso_ts):
    try:
        dt = datetime.fromisoformat(iso_ts)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return 999999

def _read_q():
    """Lee la cola desde GitHub. Retorna (data, sha)."""
    url = f"https://api.github.com/repos/{_repo()}/contents/{QUEUE_FILE}"
    try:
        r = requests.get(url, headers=_headers(), params={"ref": _branch()}, timeout=10)
        if r.status_code == 404:
            return {"processing": None, "waiting": []}, None
        if r.status_code == 200:
            j = r.json()
            return json.loads(base64.b64decode(j["content"]).decode()), j["sha"]
    except Exception:
        pass
    return {"processing": None, "waiting": []}, None

def _write_q(data, sha):
    """Escribe la cola en GitHub. Retorna (ok, new_sha)."""
    url = f"https://api.github.com/repos/{_repo()}/contents/{QUEUE_FILE}"
    payload = {
        "message": "queue update",
        "content": base64.b64encode(
            json.dumps(data, indent=2, ensure_ascii=False).encode()
        ).decode(),
        "branch": _branch(),
    }
    if sha:
        payload["sha"] = sha
    try:
        r = requests.put(url, headers=_headers(), json=payload, timeout=10)
        if r.status_code in (200, 201):
            return True, r.json()["content"]["sha"]
    except Exception:
        pass
    return False, None

def _clean(data):
    """Expira locks vencidos y entradas viejas. Promueve al siguiente si corresponde."""
    proc = data.get("processing")
    if proc and _elapsed_secs(proc.get("since", "")) > LOCK_TIMEOUT_SECS:
        data["processing"] = None

    data["waiting"] = [
        w for w in data.get("waiting", [])
        if _elapsed_secs(w.get("joined", "")) < WAIT_TIMEOUT_SECS
    ]

    if not data.get("processing") and data.get("waiting"):
        nxt = data["waiting"][0]
        data["waiting"] = data["waiting"][1:]
        data["processing"] = {**nxt, "since": _now()}

    return data

# ─── COLA API ─────────────────────────────────────────────

def request_turn(user: str, operation: str):
    """
    Solicita turno en la cola.
    Retorna (is_my_turn: bool, ticket: str, data: dict).
    """
    ticket = str(uuid.uuid4())[:8]
    for _ in range(6):
        data, sha = _read_q()
        data = _clean(data)

        if not data.get("processing"):
            data["processing"] = {
                "ticket": ticket, "user": user,
                "operation": operation, "since": _now(),
            }
            ok, _ = _write_q(data, sha)
            if ok:
                return True, ticket, data
        else:
            waiting = data.get("waiting", [])
            if not any(w["ticket"] == ticket for w in waiting):
                data["waiting"] = waiting + [{
                    "ticket": ticket, "user": user,
                    "operation": operation, "joined": _now(),
                }]
                ok, _ = _write_q(data, sha)
                if ok:
                    return False, ticket, data
        time.sleep(0.5)
    return True, ticket, {}  # fallback: permitir sin cola


def check_turn(ticket: str):
    """
    Verifica posición en la cola.
    Retorna (is_my_turn: bool, position: int, data: dict).
    """
    data, _ = _read_q()
    data = _clean(data)
    proc = data.get("processing")
    if proc and proc.get("ticket") == ticket:
        return True, 0, data
    for i, w in enumerate(data.get("waiting", [])):
        if w.get("ticket") == ticket:
            return False, i + 1, data
    if not proc:
        return True, 0, data
    return False, -1, data


def release_turn(ticket: str):
    """Libera el turno y promueve el siguiente en cola."""
    for _ in range(5):
        data, sha = _read_q()
        proc = data.get("processing")
        if proc and proc.get("ticket") == ticket:
            data["processing"] = None
        data["waiting"] = [w for w in data.get("waiting", []) if w.get("ticket") != ticket]
        if not data.get("processing") and data.get("waiting"):
            nxt = data["waiting"][0]
            data["waiting"] = data["waiting"][1:]
            data["processing"] = {**nxt, "since": _now()}
        ok, _ = _write_q(data, sha)
        if ok:
            return
        time.sleep(0.3)


def cancel_turn(ticket: str):
    """Cancela y elimina el ticket de la cola."""
    for _ in range(3):
        data, sha = _read_q()
        data["waiting"] = [w for w in data.get("waiting", []) if w.get("ticket") != ticket]
        proc = data.get("processing")
        if proc and proc.get("ticket") == ticket:
            data["processing"] = None
            if data.get("waiting"):
                nxt = data["waiting"][0]
                data["waiting"] = data["waiting"][1:]
                data["processing"] = {**nxt, "since": _now()}
        ok, _ = _write_q(data, sha)
        if ok:
            return
        time.sleep(0.3)


# ─── STREAMLIT INTEGRATION ────────────────────────────────

_QS = "_q_state"      # 'requesting' | 'waiting' | 'executing'
_QT = "_q_ticket"
_QO = "_q_op_id"
_QD = "_q_op_desc"
_QP = "_q_op_params"


def submit_op(op_id: str, op_desc: str, params: dict):
    """
    Registra una operación pendiente para ejecutarse vía cola.
    Llamar cuando el usuario hace clic en un botón de escritura.
    """
    st.session_state[_QS] = "requesting"
    st.session_state[_QO] = op_id
    st.session_state[_QD] = op_desc
    st.session_state[_QP] = params
    st.session_state.pop(_QT, None)


def clear_op():
    """Limpia el estado de cola."""
    for k in [_QS, _QT, _QO, _QD, _QP]:
        st.session_state.pop(k, None)


def handle_queue(op_handlers: dict):
    """
    Procesa la cola. Llamar al INICIO de la vista.
    op_handlers: {'op_id': callable(**params)}
    Retorna (completed: bool, result: any).
    Si completed=True, la operación terminó exitosamente.
    """
    state = st.session_state.get(_QS)
    if not state:
        return False, None

    user    = st.session_state.get("usuario", "")
    op_id   = st.session_state.get(_QO, "")
    op_desc = st.session_state.get(_QD, "")
    params  = st.session_state.get(_QP, {})
    ticket  = st.session_state.get(_QT)

    # ── REQUESTING ────────────────────────────────────────
    if state == "requesting":
        with st.spinner("Verificando cola..."):
            is_turn, ticket, data = request_turn(user, op_desc)
        st.session_state[_QT] = ticket
        st.session_state[_QS] = "executing" if is_turn else "waiting"
        st.rerun()

    # ── WAITING ───────────────────────────────────────────
    if state == "waiting":
        is_turn, position, data = check_turn(ticket)
        if is_turn:
            st.session_state[_QS] = "executing"
            st.rerun()

        proc    = data.get("processing") or {}
        waiting = data.get("waiting", [])
        elapsed = int(_elapsed_secs(proc.get("since", _now())))
        elapsed_str = f"{elapsed}s" if elapsed < 60 else f"{elapsed//60}m {elapsed%60}s"

        with st.container(border=True):
            st.markdown(f"### ⏳ En cola — Su posición: #{position}")
            st.caption(f"Su operación: **{op_desc}**")
            if proc:
                st.info(
                    f"🔄 Procesando ahora: **{proc.get('user')}** — "
                    f"*{proc.get('operation')}* (hace {elapsed_str})"
                )
            if len(waiting) > 1:
                st.caption("Otros en espera:")
                for w in waiting:
                    marker = "👉 **Usted**" if w.get("ticket") == ticket else f"• {w.get('user')}"
                    st.caption(f"  {marker} — {w.get('operation')}")
            col1, col2 = st.columns([4, 1])
            with col1:
                st.caption("Se actualiza automáticamente cada 4 seg...")
            with col2:
                if st.button("❌ Cancelar", key="_q_cancel"):
                    cancel_turn(ticket)
                    clear_op()
                    st.session_state["_q_cancelled"] = True
                    st.rerun()

        time.sleep(4)
        st.rerun()

    # ── EXECUTING ─────────────────────────────────────────
    if state == "executing":
        fn = op_handlers.get(op_id)
        if fn:
            try:
                with st.spinner(f"Procesando: {op_desc}..."):
                    result = fn(**params)
                release_turn(ticket)
                clear_op()
                return True, result
            except Exception as e:
                release_turn(ticket)
                clear_op()
                raise e
        release_turn(ticket)
        clear_op()

    return False, None
