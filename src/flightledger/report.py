import csv
import html
import json
from pathlib import Path

import duckdb

from .storage import atomic_json, read_json


def write_reports(root: Path) -> dict:
    published = read_json(root / "data" / "published.json")
    if not published:
        raise ValueError("No published snapshot")
    directory = root / "reports"; directory.mkdir(exist_ok=True)
    quality = (read_json(root / published["quality"]) if "quality" in published else
               [read_json(root / "data" / "quality" / f"{month}.json") for month in sorted(published["source_hashes"])])
    with duckdb.connect(str(root / published["warehouse"]), read_only=True) as conn:
        monthly_cursor = conn.execute("""select source_month, sum(flights) flights, sum(cancelled) cancelled,
            sum(operated) operated, sum(departure_observed) departure_observed,
            sum(departure_on_time) departure_on_time,
            round(100.0 * sum(cancelled)/sum(flights), 3) cancellation_pct,
            round(100.0 * sum(departure_on_time)/nullif(sum(departure_observed),0), 3) departure_otp15_pct,
            round(100.0 * sum(departure_observed)/nullif(sum(operated),0), 3) departure_coverage_pct
            from analytics.fct_daily_operations group by source_month order by source_month""")
        fields = [c[0] for c in monthly_cursor.description]
        monthly = [dict(zip(fields, row)) for row in monthly_cursor.fetchall()]
        mapping = conn.execute("""select count(*) filter(where a.airport_code is null), count(*)
            from analytics.fct_flight f left join analytics.dim_airport a on f.origin=a.airport_code""").fetchone()
        airports = conn.execute("select count(*) from analytics.dim_airport").fetchone()[0]
        routes = conn.execute("select count(*) from analytics.mart_route_reliability").fetchone()[0]
    summary = {"input_rows": sum(q["input_rows"] for q in quality), "accepted_rows": sum(q["accepted_rows"] for q in quality),
               "quarantined_rows": sum(q["quarantined_rows"] for q in quality),
               "duplicates_removed": sum(q["duplicates_removed"] for q in quality),
               "airport_dimension_rows": airports, "origin_unmatched_flights": mapping[0], "route_month_airline_rows": routes,
               "dbt_tests": published["dbt_tests"], "monthly": monthly, "quality": quality,
               "source_hashes": published["source_hashes"], "airports_hash": published["airports_hash"],
               "source": "ANAC VRA and OurAirports", "period": "2025 Q1"}
    atomic_json(directory / "validation.json", summary)
    with (directory / "monthly_operations.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader(); writer.writerows(monthly)
    with (directory / "quality.csv").open("w", encoding="utf-8", newline="") as stream:
        columns = ["period", "input_rows", "accepted_rows", "quarantined_rows", "duplicates_removed", "source_sha256"]
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore"); writer.writeheader(); writer.writerows(quality)
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="680" viewBox="0 0 1200 680" role="img" aria-labelledby="title">',
           '<title id="title">FlightLedger — carga real ANAC, primeiro trimestre de 2025</title>',
           '<rect width="1200" height="680" rx="18" fill="#10151f"/>']
    def text(x, y, label, size=15, color="#e5eaf3", weight="400"):
        svg.append(f'<text x="{x}" y="{y}" fill="{color}" font-family="DejaVu Sans,Arial,sans-serif" font-size="{size}" font-weight="{weight}">{html.escape(str(label))}</text>')
    def box(x, y, w, h, fill="#1b2433"):
        svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{fill}"/>')
    text(36, 51, "FLIGHTLEDGER", 30, weight="700")
    text(36, 80, "Lakehouse auditavel da aviacao brasileira", 18, "#a5b2c6")
    text(900, 49, "ANAC + OurAirports", 16, "#6fe4c0")
    text(900, 76, "DADOS REAIS | 2025 T1", 13, "#a5b2c6")
    cards = [("REGISTROS NA ORIGEM", summary["input_rows"]), ("REGISTROS PUBLICADOS", summary["accepted_rows"]),
             ("REGISTROS EM QUARENTENA", summary["quarantined_rows"]), ("TESTES DBT APROVADOS", summary["dbt_tests"])]
    for i, (label, value) in enumerate(cards):
        x = 32 + i * 294; box(x, 111, 276, 99)
        text(x + 15, 140, label, 12, "#a5b2c6")
        text(x + 15, 182, f"{value:,}".replace(",", "."), 31, "#6fe4c0", "700")
    box(32, 232, 1138, 238)
    text(52, 266, "QUALIDADE E COBERTURA POR PARTICAO", 17, weight="700")
    for x, label in [(52,"Mes"),(220,"Linhas"),(400,"Quarentena"),(600,"Cancelamentos"),(810,"OTP partida 15"),(1008,"Cobertura")]:
        text(x, 306, label, 13, "#a5b2c6")
    for i, (row, q) in enumerate(zip(monthly, quality)):
        y = 347 + i * 44
        for x, label in [(52,row["source_month"]),(220,f'{row["flights"]:,}'.replace(",",".")),(400,q["quarantined_rows"]),
                         (600,f'{row["cancellation_pct"]:.2f}%'),(810,f'{row["departure_otp15_pct"]:.2f}%'),(1008,f'{row["departure_coverage_pct"]:.2f}%')]:
            text(x, y, label, 16)
    box(32, 492, 555, 153); box(609, 492, 561, 153)
    text(52, 525, "HISTORICO REPROCESSAVEL", 16, "#6fe4c0", "700")
    text(52, 555, "Bronze imutavel por SHA-256; meses revisados", 15)
    text(52, 580, "substituem a particao sem duplicar o historico.", 15)
    text(52, 615, "Falha na Gold preserva a publicacao anterior.", 13, "#a5b2c6")
    text(629, 525, "PONTUALIDADE COM DENOMINADOR", 16, "#6fe4c0", "700")
    text(629, 555, "Cancelados nao entram no OTP; voos sem", 15)
    text(629, 580, "horario comparavel reduzem a cobertura.", 15)
    text(629, 615, "Horarios da ANAC: Brasilia, convertidos para UTC.", 13, "#a5b2c6")
    svg.append("</svg>")
    visual = "\n".join(svg)
    (directory / "overview.svg").write_text(visual, encoding="utf-8")
    rows = "\n".join(f"| {m['source_month']} | {m['flights']:,} | {m['cancellation_pct']:.3f}% | {m['departure_otp15_pct']:.3f}% | {m['departure_coverage_pct']:.3f}% |" for m in monthly)
    narrative = f'''# Resultado da carga real

Fonte: ANAC VRA, janeiro a março de 2025; cadastro OurAirports obtido em outubro de 2026.

- Registros de voos na origem: **{summary['input_rows']:,}**.
- Registros publicados: **{summary['accepted_rows']:,}**.
- Registros em quarentena: **{summary['quarantined_rows']}**; março contém chaves de negócio conflitantes.
- Duplicatas idênticas removidas: **{summary['duplicates_removed']}**.
- Testes dbt aprovados na publicação: **{summary['dbt_tests']}**.
- Códigos na dimensão de aeroportos: **{airports:,}**; voos com origem sem correspondência: **{mapping[0]}**.

| Mês | Etapas publicadas | Cancelamentos | OTP partida até 15 min | Cobertura de partida |
|---|---:|---:|---:|---:|
{rows}

OTP considera apenas voos realizados com ambos os horários comparáveis. Cancelamentos têm denominador próprio. Voos sem previsão permanecem na base; não recebem atraso zero. Não é um ranking de companhias nem uma medida regulatória oficial. Os valores mudam caso a fonte seja revisada.

![Carga real](overview.svg)

Os hashes e contagens estão em `validation.json`. As transformações usam arquivos oficiais sem inserir erros na carga real. Os cenários de falha, revisão e recuperação são exercitados separadamente nos testes.
'''
    (directory / "results.md").write_text(narrative, encoding="utf-8")
    (directory / "report.html").write_text(f'''<!doctype html><html lang="pt-BR"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>FlightLedger — carga real</title><style>body{{background:#10151f;color:#e5eaf3;font:16px/1.6 system-ui;margin:0}}main{{max-width:1200px;margin:auto;padding:24px}}svg{{width:100%;height:auto}}p{{max-width:950px}}code{{color:#6fe4c0}}</style><main>{visual}<h2>O que essa carga demonstra</h2><p>Ingestao mensal com checksum, padronizacao de horarios, quarentena de conflitos, particoes Delta Lake e modelos dbt publicados somente depois dos testes.</p><p>O OTP inclui apenas voos realizados com horarios comparaveis. A cobertura e publicada separadamente. O cadastro de aeroportos e atual, nao uma dimensao historica de 2025.</p><p>Dados oficiais ANAC VRA e cadastro OurAirports. Consulte <code>reports/validation.json</code> para conferir hashes e contagens, e <code>docs/runbook.md</code> para reprocessamento e recuperacao.</p></main></html>''', encoding="utf-8")
    return summary
