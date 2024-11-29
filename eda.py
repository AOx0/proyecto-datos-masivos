# %%
import polars as pl
import altair as alt

from google.cloud import bigquery

# %%
# Color palette:
CA_01 = "#D12030"
CA_02 = "#F17A3B"
CA_03 = "#9B4A97"
CA_04 = "#2E86AB"
CA_05 = "#2A4494"
CA_06 = "#5E2F50"
CA_07 = "#E76D83"
CA_08 = "#7EA8BE"
CA_09 = "#B8D4E3"
CA_10 = "#F5E6E8"
CA_11 = "#FFFFFF"

COLORS = [CA_01, CA_02, CA_03, CA_04, CA_05, CA_06, CA_07, CA_08, CA_09, CA_10]

# %%
client = bigquery.Client(project="aol-bva-examen-443118")
traffic = bigquery.DatasetReference(client.project, "traffic_data")
job_config = bigquery.QueryJobConfig()

# %%
# fecha_evento                   Type: DATE       Mode: REQUIRED
# hora_evento                    Type: TIME       Mode: NULLABLE
# tipo_evento                    Type: INTEGER    Mode: REQUIRED
# fecha_captura                  Type: DATE       Mode: NULLABLE
# folio                          Type: STRING     Mode: NULLABLE
# latitud                        Type: FLOAT      Mode: NULLABLE
# longitud                       Type: FLOAT      Mode: NULLABLE
# punto_1                        Type: STRING     Mode: REQUIRED
# punto_2                        Type: STRING     Mode: NULLABLE
# colonia                        Type: STRING     Mode: REQUIRED
# alcaldia                       Type: INTEGER    Mode: REQUIRED
# zona_vial                      Type: INTEGER    Mode: REQUIRED
# sector                         Type: STRING     Mode: REQUIRED
# unidad_cargo                   Type: STRING     Mode: NULLABLE
# tipo_interseccion              Type: INTEGER    Mode: REQUIRED
# interseccion_semaforizada      Type: BOOLEAN    Mode: REQUIRED
# clasificacion_vialidad         Type: INTEGER    Mode: NULLABLE
# sentido_circulacion            Type: INTEGER    Mode: NULLABLE
# dia                            Type: INTEGER    Mode: REQUIRED
# prioridad                      Type: INTEGER    Mode: REQUIRED
# origen                         Type: INTEGER    Mode: NULLABLE
# unidad_medica_apoyo            Type: STRING     Mode: NULLABLE
# matricula_unidad_medica        Type: STRING     Mode: NULLABLE
# trasladado_lesionados          Type: BOOLEAN    Mode: REQUIRED
# personas_fallecidas            Type: INTEGER    Mode: REQUIRED
# personas_lesionadas            Type: INTEGER    Mode: REQUIRED

events_ref = client.dataset("traffic_data").table("events")
events_table = client.get_table(events_ref)
for field in events_table.schema:
    print(f"{field.name:<30} Type: {field.field_type:<10} Mode: {field.mode}")


# %%
def query(query: str) -> pl.DataFrame:
    global client, traffic

    query_job = client.query(query.replace("#", f"{traffic}"), job_config=job_config)
    return pl.DataFrame(pl.from_arrow(query_job.result().to_arrow()))


# %%
# Gráfica de barras de personas fallecidas y lesionadas por año.
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    SUM(personas_fallecidas) as personas_fallecidas,
    SUM(personas_lesionadas) as personas_lesionadas,
 FROM `#.events`
GROUP BY anio
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("personas_fallecidas").alias("Fallecidas"))
    .with_columns(pl.col("personas_lesionadas").alias("Lesionadas"))
    .unpivot(index=["Año"], on=["Fallecidas", "Lesionadas"])
)

chart = (
    alt.Chart(res_alt)
    .mark_bar()
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y("value:Q", title="Número de Muertes"),
        color=alt.Color(
            "variable:N",
            scale=alt.Scale(range=[CA_01, CA_06]),
            legend=alt.Legend(title="Tipo"),
        ),
        tooltip=["Año", "variable", "value"],
    )
    .properties(width=600, height=400, title="Muertes por Año")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Gráfica de líneas que muestra el número de fallecimientos por tipo de evento a lo largo de años.
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    t.tipo_evento as tipo,
    SUM(personas_fallecidas) as personas_fallecidas,
 FROM `#.events` e
 JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
GROUP BY anio, tipo
""")


res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("personas_fallecidas").alias("Fallecimientos"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True, strokeWidth=3)
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y("Fallecimientos:Q", title="Número de Fallecimientos"),
        color=alt.Color(
            "Tipo:N",
            scale=alt.Scale(range=COLORS),
            legend=alt.Legend(title="Tipo de Evento"),
        ),
        tooltip=["Año", "Tipo", "Fallecimientos"],
    )
    .properties(width=600, height=400, title="Fallecimientos por Año y Tipo de Evento")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Gráfica de líneas que muestra el número de heridos por tipo de evento a lo largo de años.
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    t.tipo_evento as tipo,
    SUM(personas_lesionadas) as personas_lesionadas,
 FROM `#.events` e
 JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
GROUP BY anio, tipo
""")


res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("personas_lesionadas").alias("Lesionados"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True, strokeWidth=3)
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y(
            "Lesionados:Q", title="Número de Lesionados", axis=alt.Axis(format="s")
        ),  # Uses SI prefix format for thousands (k)
        color=alt.Color(
            "Tipo:N",
            scale=alt.Scale(range=COLORS),
            legend=alt.Legend(title="Tipo de Evento"),
        ),
        tooltip=["Año", "Tipo", "Lesionados"],
    )
    .properties(width=600, height=400, title="Lesionados por Año y Tipo de Evento")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart
