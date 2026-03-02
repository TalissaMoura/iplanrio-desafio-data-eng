from flask import Blueprint, request, jsonify
from app.db import get_connection

terceirizados_bp = Blueprint("terceirizados", __name__)

DEFAULT_LIMIT = 20
MAX_LIMIT = 200


@terceirizados_bp.route("/terceirizados", methods=["GET"])
def list_terceirizados():
    """
    Lista terceirizados com paginação
    ---
    description: |
        Retorna os valores presentes na camada gold.

        A paginação é controlada pelos parâmetros:
        - b_start
        - limit
        parameters:
        - name: b_start
            in: query
            type: integer
            required: false
        - name: limit
            in: query
            type: integer
            required: false
    responses:
        200:
            description: Lista paginada retornada com sucesso
    """
    try:
        b_start = int(request.args.get("b_start", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return jsonify({"error": "Parâmetros inválidos"}), 400

    if b_start < 0:
        return jsonify({"error": "b_start deve ser >= 0"}), 400

    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    conn = get_connection()

    total = conn.execute("SELECT COUNT(*) FROM ouro.app_terceirizados").fetchone()[0]

    rows = conn.execute(
        """
        SELECT *
        FROM ouro.app_terceirizados
        ORDER BY id_terceirizado
        LIMIT ?
        OFFSET ?
        """,
        [limit, b_start],
    ).fetchall()

    columns = [desc[0] for desc in conn.description]

    data = [dict(zip(columns, row)) for row in rows]

    conn.close()

    next_start = b_start + limit if (b_start + limit) < total else None

    return jsonify(
        {
            "b_start": b_start,
            "limit": limit,
            "total": total,
            "next": next_start,
            "data": data,
        }
    )


@terceirizados_bp.route("/terceirizados/<int:id_terceirizado>", methods=["GET"])
def get_terceirizado_by_id(id_terceirizado):
    """
    Lista registros por id_terceirizado
    ---
    parameters:
      - name: id_terceirizado
        in: path
        type: integer
        required: true
      - name: b_start
        in: query
        type: integer
        required: false
      - name: limit
        in: query
        type: integer
        required: false
    description: |
        Retorna os valores presentes na camada gold.

        A paginação é controlada pelos parâmetros:
        - b_start
        - limit
        parameters:
        - name: b_start
            in: query
            type: integer
            required: false
        - name: limit
            in: query
            type: integer
            required: false
    responses:
        200:
            description: Lista paginada retornada com sucesso por id_terceirizado.
    """
    try:
        b_start = int(request.args.get("b_start", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return jsonify({"error": "Parâmetros inválidos"}), 400

    if b_start < 0:
        return jsonify({"error": "b_start deve ser >= 0"}), 400

    if limit <= 0:
        return jsonify({"error": "limit deve ser > 0"}), 400

    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    conn = get_connection()

    # Total apenas daquele ID
    total = conn.execute(
        """
        SELECT COUNT(*)
        FROM ouro.app_terceirizados
        WHERE id_terceirizado = ?
        """,
        [id_terceirizado],
    ).fetchone()[0]

    if total == 0:
        conn.close()
        return jsonify({"error": "Nenhum registro encontrado para esse id"}), 404

    rows = conn.execute(
        """
        SELECT *
        FROM ouro.app_terceirizados
        WHERE id_terceirizado = ?
        ORDER BY id_terceirizado
        LIMIT ?
        OFFSET ?
        """,
        [id_terceirizado, limit, b_start],
    ).fetchall()

    columns = [desc[0] for desc in conn.description]
    data = [dict(zip(columns, row)) for row in rows]

    conn.close()

    next_start = b_start + limit if (b_start + limit) < total else None

    return jsonify(
        {
            "id_terceirizado": id_terceirizado,
            "b_start": b_start,
            "limit": limit,
            "total": total,
            "next": next_start,
            "data": data,
        }
    )
