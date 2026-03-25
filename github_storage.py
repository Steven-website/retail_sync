"""
Persistencia de archivos de datos en GitHub.
Cada vez que se guarda un parquet o usuarios.json, se hace commit al repo.
"""
import base64
import io
import os
import requests
import streamlit as st


def _headers() -> dict:
    token = st.secrets.get("GITHUB_TOKEN", os.environ.get("GITHUB_TOKEN", ""))
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }


def _repo() -> str:
    return st.secrets.get("GITHUB_REPO", os.environ.get("GITHUB_REPO", ""))


def _branch() -> str:
    return st.secrets.get("GITHUB_BRANCH", os.environ.get("GITHUB_BRANCH", "main"))


def _get_sha(path: str) -> str | None:
    """Obtiene el SHA del archivo en GitHub (necesario para actualizarlo)."""
    url = f"https://api.github.com/repos/{_repo()}/contents/{path}"
    r = requests.get(url, headers=_headers(), params={"ref": _branch()})
    if r.status_code == 200:
        return r.json().get("sha")
    return None


def push_bytes(contenido: bytes, path: str, mensaje: str = "update data"):
    """Sube contenido binario a GitHub como commit."""
    try:
        sha = _get_sha(path)
        url = f"https://api.github.com/repos/{_repo()}/contents/{path}"
        payload = {
            "message": mensaje,
            "content": base64.b64encode(contenido).decode("utf-8"),
            "branch": _branch(),
        }
        if sha:
            payload["sha"] = sha
        r = requests.put(url, headers=_headers(), json=payload)
        if r.status_code not in (200, 201):
            st.warning(f"⚠️ No se pudo persistir '{path}' en GitHub: {r.json().get('message', r.status_code)}")
    except Exception as e:
        st.warning(f"⚠️ Error al guardar en GitHub: {e}")


def push_parquet(df, path: str, mensaje: str = "update data"):
    """Serializa un DataFrame a parquet y lo sube a GitHub."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    push_bytes(buf.getvalue(), path, mensaje)


def push_json(texto: str, path: str, mensaje: str = "update data"):
    """Sube texto JSON a GitHub."""
    push_bytes(texto.encode("utf-8"), path, mensaje)
