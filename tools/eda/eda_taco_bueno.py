#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Análisis exploratorio (EDA) del dataset diario TLD de Taco Bueno.

Lee todos los archivos TB_Daily_TLD_MM-DD-YYYY-m.csv de una carpeta, los procesa
uno por uno (en paralelo) y acumula agregados, de modo que nunca se carga el
dataset completo (~11 GB) en memoria.

Particularidades del formato que el script toma en cuenta
---------------------------------------------------------
* El nombre del archivo es la fecha de exportación; las ventas son del día
  anterior (columna Date).
* Conviven dos sistemas POS, que se identifican por la columna Pop:
    - Legacy: Pop en {To Go, Inside, 3}. Quantity es real (0..n), Menu Price es
      precio unitario, las filas "Discount" también son ventas del artículo y
      Total = venta bruta (antes de descuento e impuesto).
    - Nuevo: Pop en {Drive Thru, Dine-In, Carry Out, Online...}. Una fila por
      unidad (Quantity = 1), Item Voided trae el precio de la línea anulada y
      Total = venta neta + impuesto.
* Los números de ticket se repiten entre tiendas: la llave del ticket es
  (Date, Store #, Check Number, Begin Time), porque el POS legacy reutilizó
  algunos números de ticket el mismo día en la misma tienda.
* Hay varios esquemas de columnas (19, 21 y 22 columnas).

Métricas armonizadas por ticket
    bruto   = venta antes de descuentos e impuestos
    neto    = bruto - descuento          (venta neta sin impuesto)
    cobrado = neto + impuesto

Uso
    python eda_taco_bueno.py                          # carpeta del script
    python eda_taco_bueno.py --datos "C:/ruta" --salida eda_output --workers 6
    python eda_taco_bueno.py --muestra 20             # prueba rápida con 20 archivos
    python eda_taco_bueno.py --sin-cache              # ignora resultados previos

Salida (carpeta --salida)
    reporte_eda.html   reporte autocontenido con hallazgos, KPIs y gráficas
    tablas/*.csv       tablas agregadas (la vista de tabla de cada gráfica)
    graficas/*.png     gráficas
    cache/             resultado por archivo, para re-ejecuciones rápidas

Requiere: pandas, numpy, matplotlib.
"""

import argparse
import base64
import html
import logging
import os
import re
import sys
import time
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #
VERSION_CACHE = 3
PATRON_ARCHIVO = re.compile(r"TB_Daily_TLD_(\d{2})-(\d{2})-(\d{4})-m\.csv$", re.I)
POP_LEGACY = {"To Go", "Inside", "3"}
TOLERANCIA = 0.02  # dólares, para validar que el ticket cuadra

COLS_TEXTO = [
    "Store #", "Date", "Check Number", "Time Card", "Begin Time", "End Time",
    "Transaction Type", "Menu Item", "Menu Item Ext Ref", "Pop", "POS Code",
    "Report Group", "Tender Type",
]
COLS_NUM = [
    "Total", "Total Discount", "Order Voided", "Tax", "Quantity", "Menu Price",
    "Item Discount", "Item Voided", "Total Rounding",
]
DIAS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]

# Paleta (validada para daltonismo en orden adyacente) y tinta de las gráficas
SERIE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
COLOR_SISTEMA = {"Nuevo": SERIE[0], "Legacy": SERIE[1]}
SUPERFICIE = "#fcfcfb"
TINTA = "#0b0b0b"
TINTA_2 = "#52514e"
TENUE = "#898781"
REJILLA = "#e1e0d9"
EJE = "#c3c2b7"
GRIS_OTRO = "#b5b3ab"
GRIS_HUECO = "#f0efec"
AZUL_CLARO = "#86b6ef"
RAMPA_AZUL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]


# --------------------------------------------------------------------------- #
# Procesamiento de un archivo (se ejecuta en procesos hijos)
# --------------------------------------------------------------------------- #
def fecha_de_nombre(nombre):
    m = PATRON_ARCHIVO.search(nombre)
    if not m:
        return None
    return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))


def _leer_csv(ruta, columnas):
    """Lee el CSV con tipos explícitos; si una columna numérica trae texto, la fuerza."""
    tipos = {c: str for c in columnas if c not in COLS_NUM}
    tipos.update({c: "float64" for c in columnas if c in COLS_NUM})
    try:
        return pd.read_csv(ruta, dtype=tipos, low_memory=False), 0
    except (ValueError, TypeError):
        df = pd.read_csv(ruta, dtype=str, low_memory=False)
        no_numericos = 0
        for c in columnas:
            if c in COLS_NUM:
                convertido = pd.to_numeric(df[c], errors="coerce")
                no_numericos += int((convertido.isna() & df[c].notna()).sum())
                df[c] = convertido
        return df, no_numericos


def _a_minutos(serie):
    """'9:31 AM' -> minutos desde medianoche (convierte solo los valores únicos)."""
    mapa = {}
    for s in serie.dropna().unique():
        try:
            t = datetime.strptime(s.strip(), "%I:%M %p")
            mapa[s] = t.hour * 60 + t.minute
        except ValueError:
            mapa[s] = np.nan
    return serie.map(mapa).astype("float64")


def _perfil_columnas(df, columnas):
    filas = []
    n = len(df)
    for c in columnas:
        s = df[c]
        r = {"columna": c, "filas": n, "nulos": int(s.isna().sum())}
        if c in COLS_NUM:
            v = s.dropna()
            r.update(
                n_num=len(v),
                suma=float(v.sum()),
                minimo=float(v.min()) if len(v) else np.nan,
                maximo=float(v.max()) if len(v) else np.nan,
                ceros=int((v == 0).sum()),
                negativos=int((v < 0).sum()),
            )
        filas.append(r)
    return pd.DataFrame(filas)


def _compactar(df):
    """Convierte columnas de texto a category para que el caché pese poco."""
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype("category")
    return df


def analizar_archivo(ruta):
    ruta = Path(ruta)
    fecha_archivo = fecha_de_nombre(ruta.name)
    columnas = list(pd.read_csv(ruta, nrows=0).columns)
    df, no_numericos = _leer_csv(ruta, columnas)
    n = len(df)

    perfil = {
        "archivo": ruta.name,
        "fecha_archivo": pd.Timestamp(fecha_archivo),
        "tamano_mb": ruta.stat().st_size / 1e6,
        "n_columnas": len(columnas),
        "columnas": "|".join(columnas),
        "filas": n,
        "valores_no_numericos": no_numericos,
    }
    perfil_cols = _perfil_columnas(df, columnas)
    perfil_cols.insert(0, "archivo", ruta.name)
    if n == 0:
        return {"perfil": perfil, "columnas": perfil_cols, "tablas": {}}

    for c in COLS_TEXTO + COLS_NUM:
        if c not in df.columns:
            df[c] = np.nan

    # --- Fecha de negocio ---------------------------------------------------
    mapa_fechas = {s: pd.to_datetime(s, format="%m/%d/%Y", errors="coerce") for s in df["Date"].dropna().unique()}
    df["fecha"] = df["Date"].map(mapa_fechas)
    fechas_invalidas = int(df["fecha"].isna().sum())
    if fechas_invalidas:
        df["fecha"] = df["fecha"].fillna(pd.Timestamp(fecha_archivo) - pd.Timedelta(days=1))
    df["fecha"] = df["fecha"].astype("datetime64[ns]")
    df["mes"] = df["fecha"].dt.to_period("M").astype(str)

    # --- Sistema POS y medidas por línea ------------------------------------
    df["Pop"] = df["Pop"].fillna("(vacío)")
    df["sistema"] = np.where(df["Pop"].isin(POP_LEGACY), "Legacy", "Nuevo")
    legacy = (df["sistema"] == "Legacy").to_numpy()
    tipo = df["Transaction Type"].fillna("")
    cantidad = df["Quantity"].fillna(0).to_numpy()
    precio = df["Menu Price"].fillna(0).to_numpy()
    anulado = df["Item Voided"].fillna(0).to_numpy()

    # Legacy: las filas Regular Sale y Discount son venta; Void no.
    # Nuevo: toda fila es venta salvo que Item Voided indique anulación.
    es_venta = np.where(legacy, tipo.isin(["Regular Sale", "Discount"]).to_numpy(), anulado <= 0)
    df["es_venta"] = es_venta
    df["unidades"] = np.where(es_venta, cantidad, 0.0)
    df["importe"] = np.where(legacy, np.where(es_venta, precio * cantidad, 0.0), precio * cantidad - anulado)
    df["item"] = df["Menu Item"].fillna("(sin nombre)")
    df["grupo"] = df["Report Group"].fillna("(sin grupo)")

    # --- Nivel ticket -------------------------------------------------------
    # El POS legacy reutilizó números de ticket el mismo día en la misma tienda
    # (72 casos en jul/oct-2025); la hora de inicio separa esas ventas.
    df["hora_inicio_llave"] = df["Begin Time"].fillna("")
    llave = ["fecha", "mes", "Store #", "Check Number", "hora_inicio_llave"]
    g = df.groupby(llave, sort=False)
    tk = g.agg(
        total=("Total", "first"),
        impuesto=("Tax", "first"),
        descuento=("Total Discount", "first"),
        pop=("Pop", "first"),
        sistema=("sistema", "first"),
        inicio=("Begin Time", "first"),
        fin=("End Time", "first"),
        tender=("Tender Type", "first"),
        lineas=("Total", "size"),
        unidades=("unidades", "sum"),
        importe_lineas=("importe", "sum"),
        anulado=("Item Voided", "sum"),
    ).reset_index()
    inconsistentes = int((g["Total"].nunique() > 1).sum())

    for c in ["total", "impuesto", "descuento", "anulado"]:
        tk[c] = tk[c].fillna(0.0)
    leg_t = tk["sistema"].eq("Legacy").to_numpy()
    tk["bruto"] = np.where(leg_t, tk["total"], tk["total"] - tk["impuesto"] + tk["descuento"])
    tk["neto"] = tk["bruto"] - tk["descuento"]
    tk["cobrado"] = tk["neto"] + tk["impuesto"]
    esperado = np.where(leg_t, tk["importe_lineas"], tk["importe_lineas"] - tk["descuento"] + tk["impuesto"])
    # En centavos enteros: el ruido de punto flotante no debe decidir una diferencia de 2 centavos.
    tk["cuadra"] = (tk["total"] - esperado).abs().round(2) < TOLERANCIA
    tk["con_descuento"] = tk["descuento"] > 0
    tk["cero"] = tk["total"].abs() < 0.005
    ini, fin = _a_minutos(tk["inicio"]), _a_minutos(tk["fin"])
    tk["hora"] = (ini // 60).fillna(-1).astype(int)
    tk["duracion_min"] = (fin - ini) % 1440

    medidas = dict(
        tickets=("total", "size"),
        bruto=("bruto", "sum"),
        descuento=("descuento", "sum"),
        neto=("neto", "sum"),
        impuesto=("impuesto", "sum"),
        cobrado=("cobrado", "sum"),
        unidades=("unidades", "sum"),
        tickets_con_descuento=("con_descuento", "sum"),
    )
    tablas = {}
    tablas["diario"] = tk.groupby(["fecha", "sistema"]).agg(
        **medidas,
        lineas=("lineas", "sum"),
        anulado=("anulado", "sum"),
        tickets_cero=("cero", "sum"),
        tickets_cuadran=("cuadra", "sum"),
    ).reset_index()
    tablas["tienda_dia"] = tk.groupby(["fecha", "Store #", "sistema"]).agg(**medidas).reset_index()
    tablas["canal"] = tk.groupby(["mes", "sistema", "pop"]).agg(
        tickets=("total", "size"), neto=("neto", "sum"), unidades=("unidades", "sum")
    ).reset_index()
    tablas["hora"] = tk.groupby(["fecha", "sistema", "hora"]).agg(
        tickets=("total", "size"), neto=("neto", "sum")
    ).reset_index()
    con_tender = tk[tk["tender"].notna()]
    tablas["tender"] = con_tender.groupby(["mes", "tender"]).agg(
        tickets=("total", "size"), cobrado=("cobrado", "sum")
    ).reset_index()

    # Histogramas por ticket (bins de $1, unidades y minutos)
    tk["bin_neto"] = np.floor(tk["neto"].clip(-20, 150)).astype(int)
    tablas["hist_ticket"] = tk.groupby(["mes", "sistema", "bin_neto"]).size().rename("tickets").reset_index()
    tk["bin_unidades"] = tk["unidades"].clip(0, 40).round().astype(int)
    tablas["hist_unidades"] = tk.groupby(["mes", "sistema", "bin_unidades"]).size().rename("tickets").reset_index()
    tk["bin_duracion"] = tk["duracion_min"].clip(0, 60).fillna(-1).astype(int)
    tablas["hist_duracion"] = tk.groupby(["mes", "sistema", "pop", "bin_duracion"]).size().rename("tickets").reset_index()

    # --- Productos ----------------------------------------------------------
    venta = df[df["es_venta"]]
    llave_item = ["fecha", "sistema", "item", "grupo"]
    items = venta.groupby(llave_item).agg(
        unidades=("unidades", "sum"),
        importe=("importe", "sum"),
        lineas=("importe", "size"),
        descuento_linea=("Item Discount", "sum"),
    )
    tickets_item = (
        venta.drop_duplicates(["item", "grupo", "Store #", "Check Number", "hora_inicio_llave"]).groupby(llave_item).size().rename("tickets")
    )
    tablas["items"] = items.join(tickets_item).reset_index()

    con_precio = venta[venta["Menu Price"] > 0]
    tablas["precios"] = con_precio.groupby(["mes", "Store #", "sistema", "item", "Menu Price"]).agg(
        unidades=("unidades", "sum"), lineas=("unidades", "size")
    ).reset_index().rename(columns={"Menu Price": "precio"})

    # --- Perfil del archivo -------------------------------------------------
    fechas = df["fecha"].dropna()
    tipos_tx = tipo.value_counts()
    perfil.update(
        fechas_negocio=int(fechas.nunique()),
        fecha_negocio_min=fechas.min(),
        fecha_negocio_max=fechas.max(),
        desfase_dias=(pd.Timestamp(fecha_archivo) - fechas.max()).days,
        fechas_invalidas=fechas_invalidas,
        tiendas=int(df["Store #"].nunique()),
        tickets=len(tk),
        pct_filas_legacy=float(legacy.mean() * 100),
        pct_tickets_cuadran=float(tk["cuadra"].mean() * 100),
        tickets_total_cero=int(tk["cero"].sum()),
        tickets_inconsistentes=inconsistentes,
        filas_item_sin_nombre=int(df["Menu Item"].isna().sum()),
        filas_precio_negativo=int((precio < 0).sum()),
        filas_regular_sale=int(tipos_tx.get("Regular Sale", 0)),
        filas_discount=int(tipos_tx.get("Discount", 0)),
        filas_void=int(tipos_tx.get("Void", 0)),
        cantidad_max=float(cantidad.max()),
        valores_pop="|".join(sorted(df["Pop"].unique())),
        neto=float(tk["neto"].sum()),
    )
    return {
        "perfil": perfil,
        "columnas": perfil_cols,
        "tablas": {k: _compactar(v) for k, v in tablas.items()},
    }


def procesar_archivo(ruta, dir_cache):
    """Envoltura con caché: reutiliza el resultado si el archivo no cambió."""
    ruta = Path(ruta)
    st = ruta.stat()
    firma = (VERSION_CACHE, st.st_size, int(st.st_mtime))
    archivo_cache = Path(dir_cache) / (ruta.stem + ".pkl.gz") if dir_cache else None
    if archivo_cache is not None and archivo_cache.exists():
        try:
            res = pd.read_pickle(archivo_cache)
            if res.get("_firma") == firma:
                res["_desde_cache"] = True
                return res
        except Exception:
            pass
    try:
        res = analizar_archivo(ruta)
    except Exception as e:  # un archivo dañado no detiene el análisis
        res = {
            "perfil": {"archivo": ruta.name, "fecha_archivo": pd.Timestamp(fecha_de_nombre(ruta.name)),
                       "tamano_mb": st.st_size / 1e6, "filas": 0,
                       "error": f"{type(e).__name__}: {e}", "traza": traceback.format_exc(limit=3)},
            "columnas": pd.DataFrame(),
            "tablas": {},
        }
        return res
    res["_firma"] = firma
    if archivo_cache is not None:
        pd.to_pickle(res, archivo_cache)
    return res


# --------------------------------------------------------------------------- #
# Acumulación de resultados parciales
# --------------------------------------------------------------------------- #
class Acumulador:
    """Concatena tablas parciales y las re-agrega periódicamente para acotar memoria."""

    def __init__(self, llaves, cada=40):
        self.llaves = llaves
        self.cada = cada
        self.partes = []

    def agregar(self, df):
        if df is None or df.empty:
            return
        self.partes.append(df)
        if len(self.partes) >= self.cada:
            self._reducir()

    def _reducir(self):
        df = pd.concat(self.partes, ignore_index=True)
        for c in self.llaves:
            if isinstance(df[c].dtype, pd.CategoricalDtype):
                df[c] = df[c].astype(object)
        self.partes = [df.groupby(self.llaves, sort=False, dropna=False).sum(numeric_only=True).reset_index()]

    def resultado(self):
        if not self.partes:
            return pd.DataFrame(columns=self.llaves)
        self._reducir()
        return self.partes[0]


LLAVES = {
    "diario": ["fecha", "sistema"],
    "tienda_dia": ["fecha", "Store #", "sistema"],
    "canal": ["mes", "sistema", "pop"],
    "hora": ["fecha", "sistema", "hora"],
    "tender": ["mes", "tender"],
    "hist_ticket": ["mes", "sistema", "bin_neto"],
    "hist_unidades": ["mes", "sistema", "bin_unidades"],
    "hist_duracion": ["mes", "sistema", "pop", "bin_duracion"],
    "items": ["fecha", "sistema", "item", "grupo"],
    "precios": ["mes", "Store #", "sistema", "item", "precio"],
}


# --------------------------------------------------------------------------- #
# Construcción de tablas finales
# --------------------------------------------------------------------------- #
def _div(a, b):
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.where(b != 0, a / b, np.nan)
    return r


def _calendario(fechas):
    if len(fechas) == 0:
        return pd.DatetimeIndex([])
    return pd.date_range(min(fechas), max(fechas), freq="D")


def construir_tablas(perfiles, perfil_cols, acum):
    T = {}

    # 1. Inventario de archivos -------------------------------------------------
    inv = pd.DataFrame(perfiles).sort_values("fecha_archivo").reset_index(drop=True)
    if "error" not in inv:
        inv["error"] = np.nan
    esquemas = [c for c in inv["columnas"].dropna().unique()]
    esquemas.sort(key=lambda c: inv.loc[inv["columnas"] == c, "fecha_archivo"].min())
    etiqueta = {c: f"{chr(65 + i)} ({len(c.split('|'))} col)" for i, c in enumerate(esquemas)}
    inv["esquema"] = inv["columnas"].map(etiqueta)
    inv["estado"] = np.select(
        [inv["error"].notna(), inv["filas"] == 0, inv["filas"] < 5000],
        ["error", "vacío (solo encabezado)", "parcial (<5,000 filas)"],
        default="ok",
    )
    T["inventario_archivos"] = inv.drop(columns=["columnas"])

    esq_rows = []
    for c in esquemas:
        sub = inv[inv["columnas"] == c]
        esq_rows.append({
            "esquema": etiqueta[c],
            "archivos": len(sub),
            "primer_archivo": sub["fecha_archivo"].min().date(),
            "ultimo_archivo": sub["fecha_archivo"].max().date(),
            "filas": int(sub["filas"].sum()),
            "columnas_extra": ", ".join(x for x in c.split("|") if x not in esquemas[0].split("|")) or "—",
        })
    T["esquemas"] = pd.DataFrame(esq_rows)

    # 2. Perfil de columnas -----------------------------------------------------
    if len(perfil_cols):
        pc = pd.concat(perfil_cols, ignore_index=True)
        agg = {"archivo": "nunique", "filas": "sum", "nulos": "sum"}
        for c in ["n_num", "suma", "ceros", "negativos"]:
            if c in pc:
                agg[c] = "sum"
        if "minimo" in pc:
            agg.update(minimo="min", maximo="max")
        prof = pc.groupby("columna", sort=False).agg(agg).reset_index().rename(columns={"archivo": "archivos_con_columna"})
        prof["pct_nulos"] = _div(prof["nulos"], prof["filas"]) * 100
        if "n_num" in prof:
            prof["media"] = _div(prof["suma"], prof["n_num"])
            prof["pct_ceros"] = _div(prof["ceros"], prof["n_num"]) * 100
        prof["sin_informacion"] = (prof["pct_nulos"] >= 99.99) | (
            prof.get("pct_ceros", pd.Series(0, index=prof.index)).fillna(0) >= 99.99
        )
        T["perfil_columnas"] = prof
    else:
        T["perfil_columnas"] = pd.DataFrame()

    # 3. Ventas diarias y cobertura ---------------------------------------------
    diario = acum["diario"].resultado()
    td = acum["tienda_dia"].resultado()
    if diario.empty:
        return T
    diario["fecha"] = pd.to_datetime(diario["fecha"])
    td["fecha"] = pd.to_datetime(td["fecha"])
    num = [c for c in diario.columns if c not in ("fecha", "sistema")]
    dtot = diario.groupby("fecha")[num].sum()
    dtot = dtot.join(td.groupby("fecha")["Store #"].nunique().rename("tiendas"))
    dtot = dtot.join(td[td["sistema"] == "Legacy"].groupby("fecha")["Store #"].nunique().rename("tiendas_legacy"))
    dtot = dtot.join(td[td["sistema"] == "Nuevo"].groupby("fecha")["Store #"].nunique().rename("tiendas_nuevo"))
    dtot[["tiendas_legacy", "tiendas_nuevo"]] = dtot[["tiendas_legacy", "tiendas_nuevo"]].fillna(0).astype(int)
    dtot["ticket_promedio"] = _div(dtot["neto"], dtot["tickets"])
    dtot["venta_por_tienda"] = _div(dtot["neto"], dtot["tiendas"])
    dtot["pct_descuento"] = _div(dtot["descuento"], dtot["bruto"]) * 100
    dtot["pct_tickets_cuadran"] = _div(dtot["tickets_cuadran"], dtot["tickets"]) * 100
    dtot["dia_semana"] = [DIAS[d] for d in dtot.index.dayofweek]
    T["ventas_diarias"] = dtot.reset_index()

    cal = _calendario(dtot.index)
    # Días sin ventas (o con muy pocas): ¿no hubo archivo o el archivo vino vacío?
    archivos_por_fecha = {
        (r.fecha_archivo - pd.Timedelta(days=1)).normalize(): r.estado for r in inv.itertuples()
    }
    umbral = dtot["tickets"].median() * 0.2
    huecos = []
    for d in cal:
        tks = dtot["tickets"].get(d, 0)
        if tks < umbral:
            estado = archivos_por_fecha.get(d, "sin archivo")
            huecos.append({"fecha_negocio": d.date(), "dia_semana": DIAS[d.dayofweek],
                           "tickets": int(tks), "archivo_esperado": f"TB_Daily_TLD_{(d + pd.Timedelta(days=1)):%m-%d-%Y}-m.csv",
                           "situacion": estado if tks == 0 else "parcial"})
    T["dias_sin_datos"] = pd.DataFrame(huecos, columns=["fecha_negocio", "dia_semana", "tickets", "archivo_esperado", "situacion"])
    # Fechas de negocio presentes en más de un archivo (posible duplicado)
    rep = inv.dropna(subset=["fecha_negocio_min"])
    dup = rep.groupby("fecha_negocio_max")["archivo"].apply(list)
    T["fechas_duplicadas"] = dup[dup.str.len() > 1].reset_index()

    # 4. Ventas mensuales por sistema ------------------------------------------
    diario["mes"] = diario["fecha"].dt.to_period("M").astype(str)
    td["mes"] = td["fecha"].dt.to_period("M").astype(str)
    mens = diario.groupby(["mes", "sistema"])[num].sum()
    mens = mens.join(td.groupby(["mes", "sistema"]).agg(
        tiendas=("Store #", "nunique"), tienda_dias=("Store #", "size"), dias=("fecha", "nunique")))
    mens = mens.reset_index()
    mens["ticket_promedio"] = _div(mens["neto"], mens["tickets"])
    mens["unidades_por_ticket"] = _div(mens["unidades"], mens["tickets"])
    mens["venta_por_tienda_dia"] = _div(mens["neto"], mens["tienda_dias"])
    mens["pct_tickets_con_descuento"] = _div(mens["tickets_con_descuento"], mens["tickets"]) * 100
    mens["pct_descuento_s_bruto"] = _div(mens["descuento"], mens["bruto"]) * 100
    mens["pct_tickets_cuadran"] = _div(mens["tickets_cuadran"], mens["tickets"]) * 100
    T["ventas_mensuales_sistema"] = mens

    mt = diario.groupby("mes")[num].sum().join(
        td.groupby("mes").agg(tiendas=("Store #", "nunique"), tienda_dias=("Store #", "size"), dias=("fecha", "nunique"))
    ).reset_index()
    mt["ticket_promedio"] = _div(mt["neto"], mt["tickets"])
    mt["venta_por_tienda_dia"] = _div(mt["neto"], mt["tienda_dias"])
    mt["tickets_por_tienda_dia"] = _div(mt["tickets"], mt["tienda_dias"])
    T["ventas_mensuales"] = mt

    # Comparación interanual (mismo mes, normalizado por tienda-día)
    mt["anio"] = mt["mes"].str[:4].astype(int)
    mt["num_mes"] = mt["mes"].str[5:].astype(int)
    yoy = []
    for m, sub in mt.groupby("num_mes"):
        sub = sub.sort_values("anio")
        for a in range(len(sub) - 1):
            p, c = sub.iloc[a], sub.iloc[a + 1]
            if c["anio"] - p["anio"] != 1:
                continue
            yoy.append({
                "mes": m, "anio_previo": p["anio"], "anio_actual": c["anio"],
                "dias_previo": p["dias"], "dias_actual": c["dias"],
                "venta_tienda_dia_previo": p["venta_por_tienda_dia"], "venta_tienda_dia_actual": c["venta_por_tienda_dia"],
                "var_venta_tienda_dia_pct": (c["venta_por_tienda_dia"] / p["venta_por_tienda_dia"] - 1) * 100,
                "ticket_prom_previo": p["ticket_promedio"], "ticket_prom_actual": c["ticket_promedio"],
                "var_ticket_pct": (c["ticket_promedio"] / p["ticket_promedio"] - 1) * 100,
                "tickets_tienda_dia_previo": p["tickets_por_tienda_dia"], "tickets_tienda_dia_actual": c["tickets_por_tienda_dia"],
                "var_tickets_tienda_dia_pct": (c["tickets_por_tienda_dia"] / p["tickets_por_tienda_dia"] - 1) * 100,
                "cobertura_completa": bool(p["dias"] >= 25 and c["dias"] >= 25),
            })
    T["comparacion_interanual"] = pd.DataFrame(yoy)

    # 5. Tiendas ----------------------------------------------------------------
    activos = td[td["tickets"] > 0]
    st = activos.groupby("Store #").agg(
        primera_fecha=("fecha", "min"), ultima_fecha=("fecha", "max"), dias_con_venta=("fecha", "nunique"),
        tickets=("tickets", "sum"), bruto=("bruto", "sum"), descuento=("descuento", "sum"), neto=("neto", "sum"),
        unidades=("unidades", "sum"),
    )
    st = st.join(activos[activos["sistema"] == "Legacy"].groupby("Store #")["fecha"].agg(dias_legacy="nunique", ultima_fecha_legacy="max"))
    st = st.join(activos[activos["sistema"] == "Nuevo"].groupby("Store #")["fecha"].agg(dias_nuevo="nunique", fecha_migracion="min"))
    st[["dias_legacy", "dias_nuevo"]] = st[["dias_legacy", "dias_nuevo"]].fillna(0).astype(int)
    st["venta_diaria_prom"] = _div(st["neto"], st["dias_con_venta"])
    st["tickets_diarios_prom"] = _div(st["tickets"], st["dias_con_venta"])
    st["ticket_promedio"] = _div(st["neto"], st["tickets"])
    st["pct_descuento"] = _div(st["descuento"], st["bruto"]) * 100
    fmin, fmax = dtot.index.min(), dtot.index.max()
    st["observacion"] = np.select(
        [st["primera_fecha"] > fmin + pd.Timedelta(days=14), st["ultima_fecha"] < fmax - pd.Timedelta(days=14)],
        ["aparece después del inicio", "sin datos recientes"], default="",
    )
    st["cobertura_pct"] = st["dias_con_venta"] / ((st["ultima_fecha"] - st["primera_fecha"]).dt.days + 1) * 100
    T["tiendas"] = st.sort_values("neto", ascending=False).reset_index()
    T["tienda_dia"] = td.drop(columns=["mes"])

    # 6. Canal (Pop) y medio de pago -------------------------------------------
    canal = acum["canal"].resultado()
    if not canal.empty:
        canal["pct_tickets_mes_sistema"] = canal["tickets"] / canal.groupby(["mes", "sistema"])["tickets"].transform("sum") * 100
        canal["ticket_promedio"] = _div(canal["neto"], canal["tickets"])
        T["canal_mensual"] = canal.sort_values(["mes", "sistema", "tickets"], ascending=[True, True, False])
        res = canal.groupby(["sistema", "pop"])[["tickets", "neto", "unidades"]].sum().reset_index()
        res["pct_tickets_sistema"] = res["tickets"] / res.groupby("sistema")["tickets"].transform("sum") * 100
        res["ticket_promedio"] = _div(res["neto"], res["tickets"])
        T["canal_resumen"] = res.sort_values(["sistema", "tickets"], ascending=[True, False])
    tender = acum["tender"].resultado()
    if not tender.empty:
        tr = tender.groupby("tender")[["tickets", "cobrado"]].sum().sort_values("tickets", ascending=False).reset_index()
        tr["pct_tickets"] = tr["tickets"] / tr["tickets"].sum() * 100
        tr["meses_con_dato"] = tr["tender"].map(tender.groupby("tender")["mes"].nunique())
        T["medio_pago"] = tr
        T["medio_pago_cobertura"] = tender.groupby("mes")["tickets"].sum().rename("tickets_con_medio_pago").reset_index()

    # 7. Patrones por hora y día de la semana ----------------------------------
    hora = acum["hora"].resultado()
    if not hora.empty:
        hora["fecha"] = pd.to_datetime(hora["fecha"])
        hora["dow"] = hora["fecha"].dt.dayofweek
        n_dias = dtot.reset_index().assign(dow=lambda x: x["fecha"].dt.dayofweek).groupby("dow")["fecha"].nunique()
        hd = hora[hora["hora"] >= 0].groupby(["dow", "hora"])[["tickets", "neto"]].sum().reset_index()
        hd["tickets_prom_dia"] = hd["tickets"] / hd["dow"].map(n_dias)
        hd["neto_prom_dia"] = hd["neto"] / hd["dow"].map(n_dias)
        hd["dia_semana"] = hd["dow"].map(dict(enumerate(DIAS)))
        T["hora_dia_semana"] = hd
        hs = hora[hora["hora"] >= 0].groupby(["sistema", "hora"])[["tickets", "neto"]].sum().reset_index()
        hs["pct_tickets"] = hs["tickets"] / hs.groupby("sistema")["tickets"].transform("sum") * 100
        T["hora_sistema"] = hs
    dw = dtot.reset_index()
    dw["dow"] = dw["fecha"].dt.dayofweek
    dw = dw[dw["tickets"] >= umbral]
    dsem = dw.groupby("dow").agg(dias=("fecha", "size"), neto_prom=("neto", "mean"), tickets_prom=("tickets", "mean"),
                                 venta_por_tienda_prom=("venta_por_tienda", "mean")).reset_index()
    dsem["dia_semana"] = dsem["dow"].map(dict(enumerate(DIAS)))
    dsem["ticket_promedio"] = _div(dsem["neto_prom"], dsem["tickets_prom"])
    T["dia_semana"] = dsem

    # 8. Distribuciones por ticket ---------------------------------------------
    for nombre, col in [("hist_ticket", "bin_neto"), ("hist_unidades", "bin_unidades")]:
        h = acum[nombre].resultado()
        if h.empty:
            continue
        h = h.groupby(["sistema", col])["tickets"].sum().reset_index()
        h["pct"] = h["tickets"] / h.groupby("sistema")["tickets"].transform("sum") * 100
        T[nombre] = h.sort_values(["sistema", col])
    hdur = acum["hist_duracion"].resultado()
    if not hdur.empty:
        hdur = hdur[hdur["bin_duracion"] >= 0]
        dur = hdur.groupby(["sistema", "pop", "bin_duracion"])["tickets"].sum().reset_index()
        filas = []
        for (s, p), sub in dur.groupby(["sistema", "pop"]):
            sub = sub.sort_values("bin_duracion")
            acumulado = sub["tickets"].cumsum() / sub["tickets"].sum()
            filas.append({
                "sistema": s, "pop": p, "tickets": int(sub["tickets"].sum()),
                "mediana_min": int(sub["bin_duracion"].iloc[np.searchsorted(acumulado.to_numpy(), 0.5)]),
                "p90_min": int(sub["bin_duracion"].iloc[min(np.searchsorted(acumulado.to_numpy(), 0.9), len(sub) - 1)]),
                "pct_mas_de_10_min": float(sub.loc[sub["bin_duracion"] > 10, "tickets"].sum() / sub["tickets"].sum() * 100),
            })
        T["duracion_ticket"] = pd.DataFrame(filas).sort_values(["sistema", "tickets"], ascending=[True, False])

    # 9. Productos --------------------------------------------------------------
    items = acum["items"].resultado()
    if not items.empty:
        items["fecha"] = pd.to_datetime(items["fecha"])
        ir = items.groupby(["sistema", "item"]).agg(
            unidades=("unidades", "sum"), importe=("importe", "sum"), tickets=("tickets", "sum"),
            lineas=("lineas", "sum"), descuento_linea=("descuento_linea", "sum"),
            primera_fecha=("fecha", "min"), ultima_fecha=("fecha", "max"), dias_vendido=("fecha", "nunique"),
        )
        grupo_principal = (items.groupby(["sistema", "item", "grupo"])["unidades"].sum().reset_index()
                           .sort_values("unidades").drop_duplicates(["sistema", "item"], keep="last")
                           .set_index(["sistema", "item"])["grupo"])
        ir = ir.join(grupo_principal).reset_index()
        tickets_sistema = diario.groupby("sistema")["tickets"].sum()
        importe_sistema = ir.groupby("sistema")["importe"].transform("sum")
        ir["pct_importe_sistema"] = _div(ir["importe"], importe_sistema) * 100
        ir["pct_tickets_con_item"] = _div(ir["tickets"], ir["sistema"].map(tickets_sistema)) * 100
        ir["precio_prom"] = _div(ir["importe"], ir["unidades"])
        ir["es_no_venta_NR"] = ir["item"].str.contains(r"\(NR\)", regex=True)
        T["items_resumen"] = ir.sort_values(["sistema", "importe"], ascending=[True, False])

        it = items.groupby("item").agg(unidades=("unidades", "sum"), importe=("importe", "sum"), tickets=("tickets", "sum"))
        sistemas_item = items.groupby("item")["sistema"].agg(lambda s: " + ".join(sorted(set(s))))
        it = it.join(sistemas_item.rename("sistemas")).reset_index()
        it["pct_importe"] = it["importe"] / it["importe"].sum() * 100
        it = it.sort_values("importe", ascending=False)
        it["pct_importe_acumulado"] = it["pct_importe"].cumsum()
        T["items_total"] = it

        ng = items[items["sistema"] == "Nuevo"]
        if not ng.empty:
            gr = ng.groupby("grupo")[["unidades", "importe", "tickets"]].sum().sort_values("importe", ascending=False).reset_index()
            gr["pct_importe"] = gr["importe"] / gr["importe"].sum() * 100
            gr["items_distintos"] = gr["grupo"].map(ng.groupby("grupo")["item"].nunique())
            T["report_group"] = gr

        im = items.assign(mes=items["fecha"].dt.to_period("M").astype(str)).groupby(["mes", "item"])[["unidades", "importe"]].sum().reset_index()
        T["items_mensual"] = im

    # 10. Precios ---------------------------------------------------------------
    precios = acum["precios"].resultado()
    if not precios.empty:
        precios = precios.sort_values(["mes", "Store #", "item", "unidades", "lineas"])
        # Precio modal por tienda-mes-producto (el precio con más unidades)
        modal = precios.drop_duplicates(["mes", "Store #", "item"], keep="last")[["mes", "Store #", "sistema", "item", "precio"]]
        modal = modal.rename(columns={"precio": "precio_modal"})
        T["precio_tienda_mes"] = modal

        pm = precios.groupby(["mes", "item"]).apply(
            lambda x: pd.Series({
                "precio_modal": x.loc[x.groupby("precio")["unidades"].transform("sum").idxmax(), "precio"],
                "precio_min": x["precio"].min(),
                "precio_max": x["precio"].max(),
                "precio_prom_pond": np.average(x["precio"], weights=x["unidades"]) if x["unidades"].sum() > 0 else x["precio"].mean(),
                "precios_distintos": x["precio"].nunique(),
                "unidades": x["unidades"].sum(),
            }), include_groups=False,
        ).reset_index()
        q = modal.groupby(["mes", "item"])["precio_modal"].quantile([0.1, 0.9]).unstack()
        q.columns = ["p10_tiendas", "p90_tiendas"]
        pm = pm.merge(q.reset_index(), on=["mes", "item"], how="left")
        pm = pm.merge(modal.groupby(["mes", "item"])["Store #"].nunique().rename("tiendas").reset_index(), on=["mes", "item"])
        T["precio_item_mensual"] = pm

        # Dispersión de precios entre tiendas en el último mes completo
        meses = sorted(modal["mes"].unique())
        dias_mes = dtot.groupby(dtot.index.to_period("M").astype(str)).size()
        completos = [m for m in meses if dias_mes.get(m, 0) >= 25]
        ultimo = completos[-1] if completos else meses[-1]
        um = modal[modal["mes"] == ultimo]
        disp = um.groupby("item")["precio_modal"].agg(
            tiendas="size", precios_distintos="nunique", precio_min="min", precio_mediana="median", precio_max="max")
        disp["rango_pct"] = (disp["precio_max"] / disp["precio_min"] - 1) * 100
        dist = um.groupby(["item", "precio_modal"]).size().rename("n").reset_index()
        dist = dist.sort_values(["item", "n"], ascending=[True, False])
        disp["niveles_de_precio"] = dist.groupby("item").apply(
            lambda x: ", ".join(f"${p:.2f} ({n})" for p, n in zip(x["precio_modal"].head(5), x["n"].head(5))),
            include_groups=False)
        disp = disp.join(T["items_total"].set_index("item")["importe"] if "items_total" in T else None)
        disp.insert(0, "mes", ultimo)
        T["dispersion_precio_tiendas"] = disp.sort_values("importe", ascending=False).reset_index()

        # Cambios de precio mes a mes por tienda-producto
        mo = modal.sort_values(["Store #", "item", "mes"]).copy()
        mo["precio_anterior"] = mo.groupby(["Store #", "item"])["precio_modal"].shift()
        mo["mes_anterior"] = mo.groupby(["Store #", "item"])["mes"].shift()
        mo = mo.dropna(subset=["precio_anterior"])
        mo = mo[(mo["precio_modal"] - mo["precio_anterior"]).abs() > 0.005]
        mo["cambio_pct"] = (mo["precio_modal"] / mo["precio_anterior"] - 1) * 100
        mo["direccion"] = np.where(mo["cambio_pct"] > 0, "aumento", "baja")
        T["cambios_precio_detalle"] = mo
        if not mo.empty:
            cp = mo.groupby(["mes", "item"]).agg(
                tiendas_aumento=("direccion", lambda s: int((s == "aumento").sum())),
                tiendas_baja=("direccion", lambda s: int((s == "baja").sum())),
                cambio_mediano_pct=("cambio_pct", "median"),
                precio_anterior_mediano=("precio_anterior", "median"),
                precio_nuevo_mediano=("precio_modal", "median"),
            ).reset_index()
            cp["tiendas_con_cambio"] = cp["tiendas_aumento"] + cp["tiendas_baja"]
            T["cambios_precio"] = cp.sort_values(["mes", "tiendas_con_cambio"], ascending=[True, False])

    # 11. Calidad por sistema ---------------------------------------------------
    cal_s = diario.groupby("sistema")[["tickets", "tickets_cuadran", "tickets_cero", "anulado", "bruto"]].sum()
    cal_s["pct_cuadran"] = _div(cal_s["tickets_cuadran"], cal_s["tickets"]) * 100
    cal_s["pct_total_cero"] = _div(cal_s["tickets_cero"], cal_s["tickets"]) * 100
    cal_s["pct_anulado_s_bruto"] = _div(cal_s["anulado"], cal_s["bruto"]) * 100
    T["calidad_sistema"] = cal_s.reset_index()
    return T


# --------------------------------------------------------------------------- #
# Gráficas
# --------------------------------------------------------------------------- #
def _estilo():
    plt.rcParams.update({
        "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "savefig.facecolor": SUPERFICIE,
        "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"], "font.size": 10,
        "text.color": TINTA, "axes.labelcolor": TINTA_2, "axes.titlesize": 12, "axes.titleweight": "semibold",
        "axes.titlelocation": "left", "axes.titlepad": 12, "axes.edgecolor": EJE, "axes.linewidth": 0.8,
        "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.axisbelow": True,
        "grid.color": REJILLA, "grid.linewidth": 0.6, "grid.linestyle": "-",
        "xtick.color": TENUE, "ytick.color": TENUE, "xtick.labelcolor": TINTA_2, "ytick.labelcolor": TINTA_2,
        "xtick.major.size": 0, "ytick.major.size": 0, "legend.frameon": False, "legend.fontsize": 9,
        "lines.linewidth": 1.8, "lines.solid_capstyle": "round",
    })


def _dinero(x, _=None):
    """Formato para texto: abrevia solo cifras grandes."""
    a = abs(x)
    if a >= 1e6:
        return f"${x / 1e6:,.1f}M"
    if a >= 1e5:
        return f"${x / 1e3:,.0f}K"
    return f"${x:,.0f}"


def _dinero_eje(x, _=None):
    """Formato para ejes: misma unidad en todas las marcas."""
    a = abs(x)
    if a >= 1e6:
        return f"${x / 1e6:,.1f}M"
    if a >= 1e3:
        return f"${x / 1e3:,.0f}K"
    return f"${x:,.0f}"


def _fig(ancho=10, alto=4.2):
    fig, ax = plt.subplots(figsize=(ancho, alto), dpi=130)
    return fig, ax


def _eje_fechas(ax):
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.grid(axis="x", visible=False)


def _sombrear_huecos(ax, serie):
    """Sombrea en gris los días del calendario sin datos."""
    faltan = serie[serie.isna()].index
    primero = True
    for d in faltan:
        ax.axvspan(d - pd.Timedelta(hours=12), d + pd.Timedelta(hours=12), color=GRIS_HUECO, lw=0,
                   zorder=0, label="Sin datos" if primero else None)
        primero = False


def graficar(T, dir_graf):
    _estilo()
    hechas = {}

    def guardar(fig, nombre, titulo, nota=""):
        ruta = dir_graf / f"{nombre}.png"
        fig.tight_layout()
        fig.savefig(ruta, bbox_inches="tight")
        plt.close(fig)
        hechas[nombre] = (ruta, titulo, nota)

    def intentar(fn):
        try:
            fn()
        except Exception as e:
            plt.close("all")
            print(f"  [aviso] no se pudo generar la gráfica {fn.__name__}: {e}")

    dv = T.get("ventas_diarias")
    if dv is None or dv.empty:
        return hechas
    dv = dv.set_index("fecha")
    cal = _calendario(dv.index)
    dvc = dv.reindex(cal)
    umbral = dv["tickets"].median() * 0.2
    dvc.loc[dvc["tickets"] < umbral] = np.nan

    def g_cobertura():
        fig, ax = _fig(10, 3.6)
        _sombrear_huecos(ax, dvc["tickets"])
        ax.bar(dvc.index, dvc["tickets"], width=0.9, color=SERIE[0], lw=0, label="Tickets del día")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / 1e3:,.0f}K"))
        ax.set_title("Tickets por día de negocio")
        ax.set_ylabel("Tickets")
        _eje_fechas(ax)
        ax.legend(loc="lower left", ncol=2)
        guardar(fig, "01_cobertura_tickets_diarios", "Cobertura: tickets por día de negocio",
                "Las franjas grises son días sin archivo, con archivo vacío o con menos del 20% de los tickets de un día típico.")

    def g_ventas():
        fig, ax = _fig()
        _sombrear_huecos(ax, dvc["neto"])
        ax.plot(dvc.index, dvc["neto"], color=AZUL_CLARO, lw=1, label="Venta neta diaria")
        media = dvc["neto"].rolling(7, min_periods=5).mean()
        ax.plot(dvc.index, media, color=SERIE[0], lw=2, label="Promedio móvil 7 días")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_dinero_eje))
        ax.set_title("Venta neta diaria (sin impuesto, después de descuentos)")
        ax.set_ylim(bottom=0)
        _eje_fechas(ax)
        ax.legend(loc="lower left", ncol=3)
        guardar(fig, "02_venta_neta_diaria", "Venta neta diaria")

    def g_migracion():
        fig, ax = _fig(10, 3.8)
        leg = dvc["tiendas_legacy"]
        nue = dvc["tiendas_nuevo"]
        ax.fill_between(cal, 0, nue, color=COLOR_SISTEMA["Nuevo"], lw=0, label="POS nuevo", step="mid")
        ax.fill_between(cal, nue, nue + leg, color=COLOR_SISTEMA["Legacy"], lw=0, label="POS legacy", step="mid")
        ax.set_title("Tiendas con venta por día, según sistema POS")
        ax.set_ylabel("Tiendas")
        ax.set_ylim(0, (nue.fillna(0) + leg.fillna(0)).max() * 1.25)
        _eje_fechas(ax)
        ax.legend(loc="upper left", ncol=2)
        guardar(fig, "03_tiendas_por_sistema", "Migración de POS: tiendas activas por sistema",
                "El sistema se infiere de la columna Pop. Las definiciones de Total, Quantity y Menu Price cambian entre sistemas.")

    mens = T.get("ventas_mensuales_sistema")

    def g_ticket():
        fig, ax = _fig(10, 3.8)
        for s in ["Legacy", "Nuevo"]:
            sub = mens[(mens["sistema"] == s) & (mens["tickets"] >= 1000)]
            if sub.empty:
                continue
            x = pd.PeriodIndex(sub["mes"], freq="M").to_timestamp()
            ax.plot(x, sub["ticket_promedio"], color=COLOR_SISTEMA[s], marker="o", ms=4, label=f"POS {s.lower()}")
            ax.annotate(f"${sub['ticket_promedio'].iloc[-1]:.2f}", (x[-1], sub["ticket_promedio"].iloc[-1]),
                        xytext=(6, 0), textcoords="offset points", va="center", fontsize=9, color=TINTA_2)
        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.2f}"))
        ax.set_title("Ticket promedio mensual (venta neta / tickets)")
        _eje_fechas(ax)
        ax.legend(loc="upper left", ncol=2)
        guardar(fig, "04_ticket_promedio_mensual", "Ticket promedio mensual por sistema",
                "Solo meses con al menos 1,000 tickets en el sistema.")

    def g_heatmap():
        hd = T["hora_dia_semana"]
        piv = hd.pivot_table(index="dow", columns="hora", values="tickets_prom_dia", aggfunc="sum")
        piv = piv.loc[:, piv.sum() > piv.values[~np.isnan(piv.values)].sum() * 0.002]
        orden = sorted(piv.columns, key=lambda h: (h - 5) % 24)  # el día operativo empieza a las 5 a.m.
        piv = piv[orden]
        fig, ax = _fig(10, 3.6)
        cmap = LinearSegmentedColormap.from_list("azul", RAMPA_AZUL)
        im = ax.imshow(piv.values, aspect="auto", cmap=cmap)
        ax.set_yticks(range(len(piv.index)), [DIAS[d] for d in piv.index])
        ax.set_xticks(range(len(orden)), [f"{h}h" for h in orden])
        ax.grid(False)
        for s in ax.spines.values():
            s.set_visible(False)
        cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
        cb.outline.set_visible(False)
        cb.ax.tick_params(labelsize=8, colors=TINTA_2)
        cb.set_label("Tickets promedio por día (todas las tiendas)", color=TINTA_2, fontsize=8)
        ax.set_title("Tickets promedio por hora de inicio y día de la semana")
        guardar(fig, "05_mapa_hora_dia", "Patrón semanal por hora")

    def g_dia_semana():
        ds = T["dia_semana"].sort_values("dow")
        fig, ax = _fig(10, 3.4)
        ax.bar(ds["dia_semana"], ds["venta_por_tienda_prom"], color=SERIE[0], width=0.6)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_dinero_eje))
        ax.grid(axis="x", visible=False)
        ax.set_title("Venta neta promedio por tienda según día de la semana")
        guardar(fig, "06_dia_semana", "Venta por día de la semana")

    def g_canal():
        cm = T["canal_mensual"]
        cm = cm[cm["sistema"] == "Nuevo"]
        if cm.empty:
            return
        top = cm.groupby("pop")["tickets"].sum().sort_values(ascending=False)
        principales = list(top.index[:6])
        cm = cm.assign(canal=np.where(cm["pop"].isin(principales), cm["pop"], "Otros"))
        piv = cm.pivot_table(index="mes", columns="canal", values="tickets", aggfunc="sum").fillna(0)
        piv = piv[piv.sum(axis=1) >= 1000]
        piv = piv.div(piv.sum(axis=1), axis=0) * 100
        columnas = principales + (["Otros"] if "Otros" in piv else [])
        fig, ax = _fig(10, 4.2)
        base = np.zeros(len(piv))
        for i, c in enumerate(columnas):
            color = SERIE[i] if c != "Otros" else GRIS_OTRO
            ax.bar(piv.index, piv[c], bottom=base, color=color, width=0.7, edgecolor=SUPERFICIE, linewidth=1.2, label=c)
            base += piv[c].to_numpy()
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
        ax.grid(axis="x", visible=False)
        ax.tick_params(axis="x", labelrotation=45)
        ax.set_title("Mezcla de canales por mes (POS nuevo, % de tickets)")
        ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
        guardar(fig, "07_mezcla_canales", "Mezcla de canales (POS nuevo)",
                "El POS legacy usa otras etiquetas (To Go, Inside, 3) que no son comparables; ver tabla canal_resumen.csv.")

    def g_dist_ticket():
        h = T["hist_ticket"]
        fig, ax = _fig(10, 3.8)
        for s in ["Legacy", "Nuevo"]:
            sub = h[(h["sistema"] == s) & (h["bin_neto"].between(0, 80))]
            if sub.empty:
                continue
            ax.step(sub["bin_neto"], sub["pct"], where="post", color=COLOR_SISTEMA[s], label=f"POS {s.lower()}")
        ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=1))
        ax.set_title("Distribución de la venta neta por ticket (intervalos de $1)")
        ax.set_ylabel("% de tickets")
        ax.legend(loc="upper right")
        guardar(fig, "08_distribucion_ticket", "Distribución del ticket")

    def g_tiendas():
        st = T["tiendas"]
        st = st[st["dias_con_venta"] >= 30]
        fig, ax = _fig(10, 3.6)
        ax.hist(st["venta_diaria_prom"], bins=25, color=SERIE[0], edgecolor=SUPERFICIE, linewidth=1.2)
        med = st["venta_diaria_prom"].median()
        ax.axvline(med, color=TINTA_2, lw=1)
        ax.annotate(f"Mediana {_dinero(med)}", (med, ax.get_ylim()[1] * 0.92), xytext=(6, 0),
                    textcoords="offset points", fontsize=9, color=TINTA_2)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_dinero_eje))
        ax.grid(axis="x", visible=False)
        ax.set_ylabel("Tiendas")
        ax.set_title("Distribución de la venta neta diaria promedio por tienda")
        guardar(fig, "09_venta_por_tienda", "Venta diaria promedio por tienda", "Tiendas con al menos 30 días con venta.")

    def g_top_items():
        it = T["items_total"]
        it = it[~it["item"].str.contains(r"\(NR\)|\(sin nombre\)", regex=True)].head(20).iloc[::-1]
        fig, ax = _fig(10, 6)
        ax.barh(it["item"], it["importe"], color=SERIE[0], height=0.65)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_dinero_eje))
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", labelsize=9)
        ax.set_title("Top 20 productos por importe (todo el periodo)")
        guardar(fig, "10_top_productos", "Top 20 productos por importe",
                "Excluye artículos de no-venta (NR) y modificadores sin nombre. Los nombres pueden diferir entre sistemas POS.")

    def g_grupos():
        gr = T.get("report_group")
        if gr is None or gr.empty:
            return
        gr = gr.iloc[::-1]
        fig, ax = _fig(10, 4.6)
        ax.barh(gr["grupo"], gr["pct_importe"], color=SERIE[0], height=0.65)
        ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
        ax.grid(axis="y", visible=False)
        ax.set_title("Participación en el importe por Report Group (POS nuevo)")
        guardar(fig, "11_report_group", "Participación por Report Group", "Report Group solo existe en el POS nuevo.")

    def g_precios():
        pm = T["precio_item_mensual"]
        it = T["items_total"]
        cand = it[~it["item"].str.contains(r"\(NR\)|\(sin nombre\)|Modifier", regex=True)]
        con_precio = set(pm.loc[pm["precio_modal"] > 0, "item"])
        top = [i for i in cand["item"] if i in con_precio][:12]
        if not top:
            return
        filas, cols = 3, 4
        fig, axs = plt.subplots(filas, cols, figsize=(11, 7.2), dpi=130, sharex=True)
        for ax, item in zip(axs.flat, top):
            sub = pm[(pm["item"] == item) & (pm["unidades"] >= 50)].sort_values("mes")
            x = pd.PeriodIndex(sub["mes"], freq="M").to_timestamp()
            ax.fill_between(x, sub["p10_tiendas"], sub["p90_tiendas"], color=RAMPA_AZUL[0], lw=0, step="mid")
            ax.step(x, sub["precio_modal"], where="mid", color=SERIE[0], lw=1.8)
            ax.set_title(item if len(item) <= 28 else item[:27] + "…", fontsize=9.5)
            ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.2f}"))
            ax.tick_params(labelsize=8)
            ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
            ax.grid(axis="x", visible=False)
            lo, hi = ax.get_ylim()
            pad = max(0.15, (hi - lo) * 0.15)
            ax.set_ylim(lo - pad, hi + pad)
        for ax in list(axs.flat)[len(top):]:
            ax.set_visible(False)
        fig.suptitle("Precio de menú modal por mes — 12 productos con mayor importe", x=0.01, ha="left",
                     fontsize=12, fontweight="semibold")
        guardar(fig, "12_evolucion_precios", "Evolución de precios de los productos principales",
                "Línea: precio modal (ponderado por unidades) de todas las tiendas. Banda: percentil 10–90 del precio modal por tienda.")

    def g_descuentos():
        m = mens[mens["tickets"] >= 1000]
        fig, axs = plt.subplots(2, 1, figsize=(10, 5.6), dpi=130, sharex=True)
        for s in ["Legacy", "Nuevo"]:
            sub = m[m["sistema"] == s]
            if sub.empty:
                continue
            x = pd.PeriodIndex(sub["mes"], freq="M").to_timestamp()
            axs[0].plot(x, sub["pct_tickets_con_descuento"], color=COLOR_SISTEMA[s], marker="o", ms=4, label=f"POS {s.lower()}")
            axs[1].plot(x, sub["pct_descuento_s_bruto"], color=COLOR_SISTEMA[s], marker="o", ms=4, label=f"POS {s.lower()}")
        axs[0].set_title("Tickets con descuento (% de tickets)")
        axs[1].set_title("Descuento como % de la venta bruta")
        for ax in axs:
            ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
            ax.set_ylim(bottom=0)
            ax.grid(axis="x", visible=False)
        _eje_fechas(axs[1])
        axs[0].legend(loc="upper right", ncol=2)
        guardar(fig, "13_descuentos", "Descuentos por mes y sistema")

    for fn in [g_cobertura, g_ventas, g_migracion, g_ticket, g_heatmap, g_dia_semana, g_canal,
               g_dist_ticket, g_tiendas, g_top_items, g_grupos, g_precios, g_descuentos]:
        intentar(fn)
    return hechas


# --------------------------------------------------------------------------- #
# Reporte HTML
# --------------------------------------------------------------------------- #
def _fmt_tabla(df, formatos=None, max_filas=25):
    if df is None or df.empty:
        return "<p class='nota'>Sin datos.</p>"
    df = df.head(max_filas).copy()
    formatos = formatos or {}
    for c in df.columns:
        f = formatos.get(c)
        if f == "$":
            df[c] = df[c].map(lambda v: f"${v:,.2f}" if pd.notna(v) else "")
        elif f == "$0":
            df[c] = df[c].map(lambda v: f"${v:,.0f}" if pd.notna(v) else "")
        elif f == "%":
            df[c] = df[c].map(lambda v: f"{v:,.1f}%" if pd.notna(v) else "")
        elif f == "n":
            df[c] = df[c].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "")
        elif f == "d":
            df[c] = pd.to_datetime(df[c]).dt.strftime("%Y-%m-%d").fillna("")
        elif pd.api.types.is_float_dtype(df[c]):
            df[c] = df[c].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "")
        elif pd.api.types.is_integer_dtype(df[c]):
            df[c] = df[c].map(lambda v: f"{v:,}")
    return "<div class='tabla'>" + df.to_html(index=False, escape=True, border=0, na_rep="") + "</div>"


def _img(ruta):
    return base64.b64encode(Path(ruta).read_bytes()).decode()


def hallazgos(T, inv):
    h = []
    ok = inv[inv["estado"] != "error"]
    vacios = inv[inv["estado"] == "vacío (solo encabezado)"]
    if len(vacios):
        h.append(f"<b>{len(vacios)} archivos vienen vacíos</b> (solo encabezado): "
                 + ", ".join(vacios["archivo"].head(8)) + ("…" if len(vacios) > 8 else "") + ".")
    errores = inv[inv["estado"] == "error"]
    if len(errores):
        h.append(f"<b>{len(errores)} archivos no se pudieron leer</b>; ver inventario_archivos.csv (columna error).")
    if "desfase_dias" in ok:
        des = ok["desfase_dias"].dropna().value_counts()
        if len(des):
            h.append(f"El nombre del archivo corresponde a la fecha de exportación: en {des.iloc[0] / des.sum() * 100:.0f}% de los "
                     f"archivos la fecha de negocio es <b>{int(des.index[0])} día(s) antes</b> del nombre.")
    esq = T.get("esquemas")
    if esq is not None and len(esq) > 1:
        h.append(f"Hay <b>{len(esq)} esquemas de columnas</b>: " + "; ".join(
            f"{r.esquema} {r.primer_archivo} → {r.ultimo_archivo} (extra: {r.columnas_extra})" for r in esq.itertuples()) + ".")
    ds = T.get("dias_sin_datos")
    if ds is not None and len(ds):
        h.append(f"<b>{len(ds)} días de negocio sin datos o incompletos</b> dentro del rango; ver dias_sin_datos.csv.")
    dv = T.get("ventas_diarias")
    if dv is not None and len(dv):
        mig = dv[dv["tiendas_nuevo"] > 0]
        if len(mig) and dv["tiendas_legacy"].sum() > 0:
            fin_leg = dv.loc[dv["tiendas_legacy"] > 0, "fecha"].max()
            h.append(f"<b>Migración de POS</b>: la primera tienda aparece en el POS nuevo el {mig['fecha'].min():%Y-%m-%d} "
                     f"y la última venta en legacy es del {fin_leg:%Y-%m-%d}. Los campos Total, Quantity y Menu Price "
                     f"significan cosas distintas en cada sistema; el script los armoniza (ver notas metodológicas).")
    cs = T.get("calidad_sistema")
    if cs is not None and len(cs):
        h.append("Reconciliación de tickets (Total vs. suma de líneas): " + "; ".join(
            f"POS {r.sistema.lower()} {r.pct_cuadran:.2f}% cuadran" for r in cs.itertuples()) + ".")
    pc = T.get("perfil_columnas")
    if pc is not None and len(pc):
        vacias = pc.loc[pc["sin_informacion"], "columna"].tolist()
        if vacias:
            h.append(f"Columnas sin información útil (100% nulas o en cero): <b>{', '.join(vacias)}</b>.")
    st = T.get("tiendas")
    if st is not None and len(st):
        nuevas = st[st["observacion"] == "aparece después del inicio"]
        cerradas = st[st["observacion"] == "sin datos recientes"]
        h.append(f"<b>{len(st)} tiendas</b> distintas; {len(nuevas)} aparecen después del inicio del periodo y "
                 f"{len(cerradas)} dejan de reportar antes del final.")
    yoy = T.get("comparacion_interanual")
    if yoy is not None and len(yoy):
        c = yoy[yoy["cobertura_completa"]]
        if len(c):
            h.append(f"Interanual (meses con cobertura completa, venta por tienda-día): mediana "
                     f"<b>{c['var_venta_tienda_dia_pct'].median():+.1f}%</b>; ticket promedio "
                     f"{c['var_ticket_pct'].median():+.1f}%; tickets por tienda-día {c['var_tickets_tienda_dia_pct'].median():+.1f}%.")
    hd = T.get("hora_dia_semana")
    if hd is not None and len(hd):
        hp = hd.groupby("hora")["tickets"].sum().idxmax()
        h.append(f"Hora pico de tickets: <b>{hp}:00–{hp + 1}:00</b>.")
    dsem = T.get("dia_semana")
    if dsem is not None and len(dsem):
        top, low = dsem.loc[dsem["venta_por_tienda_prom"].idxmax()], dsem.loc[dsem["venta_por_tienda_prom"].idxmin()]
        h.append(f"Mejor día: <b>{top['dia_semana']}</b> ({_dinero(top['venta_por_tienda_prom'])} por tienda); "
                 f"más bajo: {low['dia_semana']} ({_dinero(low['venta_por_tienda_prom'])}).")
    it = T.get("items_total")
    if it is not None and len(it):
        n80 = int((it["pct_importe_acumulado"] < 80).sum() + 1)
        h.append(f"<b>{len(it)} productos distintos</b>; {n80} concentran el 80% del importe. El principal es "
                 f"<b>{html.escape(str(it.iloc[0]['item']))}</b> ({it.iloc[0]['pct_importe']:.1f}% del importe).")
    dp = T.get("dispersion_precio_tiendas")
    if dp is not None and len(dp):
        varios = dp[(dp["precios_distintos"] > 1) & (dp["tiendas"] >= 10)]
        h.append(f"En {dp['mes'].iloc[0]}, <b>{len(varios)} productos</b> (vendidos en ≥10 tiendas) tienen más de un precio "
                 f"modal entre tiendas: hay niveles de precio por tienda.")
    cp = T.get("cambios_precio")
    if cp is not None and len(cp):
        pm = cp.groupby("mes")["tiendas_con_cambio"].sum().sort_values(ascending=False)
        h.append("Meses con más cambios de precio tienda-producto: " + ", ".join(
            f"{m} ({int(v):,})" for m, v in pm.head(3).items()) + ".")
    return h


def generar_reporte(T, graficas, ruta_html, dir_datos, segundos):
    inv = T["inventario_archivos"]
    dv = T.get("ventas_diarias", pd.DataFrame())
    st = T.get("tiendas", pd.DataFrame())
    it = T.get("items_total", pd.DataFrame())

    kpis = [
        ("Archivos", f"{len(inv):,}"),
        ("Tamaño total", f"{inv['tamano_mb'].sum() / 1e3:,.1f} GB"),
        ("Filas", f"{inv['filas'].sum() / 1e6:,.1f} M"),
    ]
    if len(dv):
        kpis += [
            ("Periodo de negocio", f"{dv['fecha'].min():%d-%b-%Y} → {dv['fecha'].max():%d-%b-%Y}"),
            ("Tiendas", f"{len(st):,}"),
            ("Tickets", f"{dv['tickets'].sum() / 1e6:,.2f} M"),
            ("Venta neta", _dinero(dv["neto"].sum())),
            ("Ticket promedio", f"${dv['neto'].sum() / dv['tickets'].sum():,.2f}"),
            ("Productos", f"{len(it):,}"),
        ]

    def fig(nombre):
        if nombre not in graficas:
            return ""
        ruta, titulo, nota = graficas[nombre]
        pie = f"<figcaption>{html.escape(nota)}</figcaption>" if nota else ""
        return (f"<figure><img src='data:image/png;base64,{_img(ruta)}' alt='{html.escape(titulo)}'>{pie}"
                f"<div class='fuente'>graficas/{nombre}.png</div></figure>")

    S = []
    S.append("<section><h2>Hallazgos principales</h2><ul class='hallazgos'>"
             + "".join(f"<li>{x}</li>" for x in hallazgos(T, inv)) + "</ul></section>")

    S.append("<section><h2>1. Inventario y calidad de datos</h2>"
             "<h3>Esquemas de columnas</h3>" + _fmt_tabla(T.get("esquemas"), {"filas": "n"})
             + "<h3>Archivos por estado</h3>"
             + _fmt_tabla(inv.groupby("estado").agg(archivos=("archivo", "size"), filas=("filas", "sum"),
                                                    gb=("tamano_mb", lambda x: x.sum() / 1e3)).reset_index(), {"filas": "n"})
             + fig("01_cobertura_tickets_diarios")
             + "<h3>Días sin datos o incompletos</h3>" + _fmt_tabla(T.get("dias_sin_datos"), max_filas=60)
             + "<h3>Reconciliación por sistema</h3>"
             + _fmt_tabla(T.get("calidad_sistema"), {"tickets": "n", "tickets_cuadran": "n", "tickets_cero": "n",
                                                     "anulado": "$0", "bruto": "$0", "pct_cuadran": "%",
                                                     "pct_total_cero": "%", "pct_anulado_s_bruto": "%"})
             + "<h3>Perfil de columnas</h3>"
             + _fmt_tabla(T.get("perfil_columnas", pd.DataFrame()).drop(columns=["suma", "n_num", "ceros"], errors="ignore"),
                          {"filas": "n", "nulos": "n", "negativos": "n", "pct_nulos": "%", "pct_ceros": "%"}, max_filas=40)
             + "</section>")

    yoy = T.get("comparacion_interanual", pd.DataFrame())
    S.append("<section><h2>2. Ventas en el tiempo</h2>" + fig("02_venta_neta_diaria") + fig("03_tiendas_por_sistema")
             + fig("04_ticket_promedio_mensual")
             + "<h3>Resumen mensual</h3>"
             + _fmt_tabla(T.get("ventas_mensuales", pd.DataFrame())[
                 ["mes", "dias", "tiendas", "tickets", "neto", "descuento", "ticket_promedio", "venta_por_tienda_dia",
                  "tickets_por_tienda_dia"]] if "ventas_mensuales" in T else None,
                 {"tickets": "n", "neto": "$0", "descuento": "$0", "ticket_promedio": "$", "venta_por_tienda_dia": "$0",
                  "tickets_por_tienda_dia": "n"}, max_filas=40)
             + "<h3>Comparación interanual (normalizada por tienda-día)</h3>"
             + _fmt_tabla(yoy[[c for c in ["mes", "anio_previo", "anio_actual", "dias_previo", "dias_actual",
                                            "venta_tienda_dia_previo", "venta_tienda_dia_actual", "var_venta_tienda_dia_pct",
                                            "var_ticket_pct", "var_tickets_tienda_dia_pct", "cobertura_completa"] if c in yoy]]
                          if len(yoy) else None,
                          {"venta_tienda_dia_previo": "$0", "venta_tienda_dia_actual": "$0", "var_venta_tienda_dia_pct": "%",
                           "var_ticket_pct": "%", "var_tickets_tienda_dia_pct": "%", "dias_previo": "n", "dias_actual": "n"})
             + fig("13_descuentos") + "</section>")

    cols_t = ["Store #", "primera_fecha", "ultima_fecha", "dias_con_venta", "fecha_migracion", "venta_diaria_prom",
              "tickets_diarios_prom", "ticket_promedio", "pct_descuento", "observacion"]
    fmt_t = {"primera_fecha": "d", "ultima_fecha": "d", "fecha_migracion": "d", "venta_diaria_prom": "$0",
             "tickets_diarios_prom": "n", "ticket_promedio": "$", "pct_descuento": "%", "dias_con_venta": "n"}
    st30 = st[st["dias_con_venta"] >= 30] if len(st) else st
    S.append("<section><h2>3. Tiendas</h2>" + fig("09_venta_por_tienda")
             + "<h3>Top 10 por venta diaria promedio</h3>"
             + _fmt_tabla(st30.sort_values("venta_diaria_prom", ascending=False)[cols_t] if len(st) else None, fmt_t, 10)
             + "<h3>Últimas 10 por venta diaria promedio</h3>"
             + _fmt_tabla(st30.sort_values("venta_diaria_prom")[cols_t] if len(st) else None, fmt_t, 10)
             + "<h3>Tiendas con cobertura parcial</h3>"
             + _fmt_tabla(st[st["observacion"] != ""][cols_t] if len(st) else None, fmt_t, 40)
             + "</section>")

    S.append("<section><h2>4. Patrones de hora y día</h2>" + fig("05_mapa_hora_dia") + fig("06_dia_semana")
             + "<h3>Duración del ticket (inicio → cierre)</h3>"
             + _fmt_tabla(T.get("duracion_ticket"), {"tickets": "n", "pct_mas_de_10_min": "%"}) + "</section>")

    S.append("<section><h2>5. Canales y medios de pago</h2>" + fig("07_mezcla_canales")
             + "<h3>Canales (Pop) por sistema</h3>"
             + _fmt_tabla(T.get("canal_resumen"), {"tickets": "n", "neto": "$0", "unidades": "n",
                                                   "pct_tickets_sistema": "%", "ticket_promedio": "$"})
             + fig("08_distribucion_ticket")
             + "<h3>Medios de pago (solo archivos que traen Tender Type)</h3>"
             + _fmt_tabla(T.get("medio_pago"), {"tickets": "n", "cobrado": "$0", "pct_tickets": "%"}, 20)
             + "</section>")

    S.append("<section><h2>6. Productos y precios</h2>" + fig("10_top_productos") + fig("11_report_group")
             + "<h3>Productos por importe</h3>"
             + _fmt_tabla(it[["item", "sistemas", "unidades", "importe", "pct_importe", "pct_importe_acumulado"]] if len(it) else None,
                          {"unidades": "n", "importe": "$0", "pct_importe": "%", "pct_importe_acumulado": "%"}, 30)
             + fig("12_evolucion_precios")
             + "<h3>Dispersión de precios entre tiendas (último mes completo)</h3>"
             + _fmt_tabla(T.get("dispersion_precio_tiendas", pd.DataFrame()).drop(columns=["importe"], errors="ignore"),
                          {"precio_min": "$", "precio_mediana": "$", "precio_max": "$", "rango_pct": "%"}, 30)
             + "<h3>Cambios de precio más extendidos (tiendas que cambiaron el precio modal vs. el mes anterior)</h3>"
             + _fmt_tabla(T.get("cambios_precio", pd.DataFrame()).sort_values("tiendas_con_cambio", ascending=False)
                          if "cambios_precio" in T else None,
                          {"cambio_mediano_pct": "%", "precio_anterior_mediano": "$", "precio_nuevo_mediano": "$"}, 30)
             + "</section>")

    S.append("""<section><h2>7. Notas metodológicas</h2><ul>
<li><b>Fecha</b>: se usa la columna Date (fecha de negocio). El nombre del archivo es la fecha de exportación.</li>
<li><b>Llave de ticket</b>: (Date, Store #, Check Number, Begin Time). Los números de ticket se repiten entre tiendas
y el POS legacy reutilizó algunos el mismo día en la misma tienda (72 casos).</li>
<li><b>Sistema POS</b>: se infiere de Pop. Legacy = {To Go, Inside, 3}; cualquier otro valor = POS nuevo.</li>
<li><b>POS legacy</b>: Menu Price es precio unitario y Quantity la cantidad real; las filas <i>Discount</i> son ventas del artículo con
descuento (Item Discount); las filas <i>Void</i> no suman. Total = venta bruta = Σ(Menu Price × Quantity) antes de descuento e impuesto.</li>
<li><b>POS nuevo</b>: una fila por unidad (Quantity = 1); Item Voided trae el precio de la línea anulada.
Total = Σ Menu Price − Σ Item Voided − Total Discount + Tax (incluye impuesto).</li>
<li><b>Métricas armonizadas</b>: bruto (antes de descuento e impuesto), neto = bruto − descuento, cobrado = neto + impuesto.
Todas las ventas del reporte son <b>netas sin impuesto</b> salvo que se indique.</li>
<li><b>Precio modal</b>: por tienda-mes-producto, el Menu Price (&gt; 0) con más unidades vendidas. Los componentes de combo con precio 0 no entran.</li>
<li>Los nombres de producto pueden diferir entre sistemas (p. ej. sufijos «(combo)» o «(Wholotta Box)» en legacy).</li>
<li>Las tablas completas de cada sección están en la carpeta <code>tablas/</code>.</li></ul></section>""")

    css = """
:root{color-scheme:light;--plano:#f9f9f7;--sup:#fcfcfb;--tinta:#0b0b0b;--tinta2:#52514e;--tenue:#898781;--linea:#e1e0d9;--acento:#2a78d6}
*{box-sizing:border-box}body{margin:0;background:var(--plano);color:var(--tinta);font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1080px;margin:0 auto;padding:40px 16px 80px}
header h1{font-size:26px;margin:0 0 4px;letter-spacing:-.01em}header p{margin:0;color:var(--tinta2)}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:28px 0 8px}
.kpi{background:var(--sup);border:1px solid rgba(11,11,11,.1);border-radius:10px;padding:14px 16px}
.kpi .v{font-size:21px;font-weight:600;line-height:1.25}.kpi .l{color:var(--tinta2);font-size:13px;margin-top:2px}
section{background:var(--sup);border:1px solid rgba(11,11,11,.1);border-radius:12px;padding:24px;margin-top:24px}
h2{font-size:19px;margin:0 0 12px}h3{font-size:15px;margin:28px 0 8px;color:var(--tinta)}
ul{padding-left:20px}li{margin:6px 0}.hallazgos li::marker{color:var(--acento)}
figure{margin:20px 0}figure img{width:100%;height:auto;display:block;border-radius:6px}
figcaption{color:var(--tinta2);font-size:13px;margin-top:6px}.fuente{color:var(--tenue);font-size:12px}
.tabla{overflow-x:auto;margin:6px 0 4px}table{border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums;width:100%}
th{font-weight:600;color:var(--tinta2);text-align:right;border-bottom:1px solid var(--tenue);padding:6px 10px;white-space:nowrap}
td{padding:5px 10px;border-bottom:1px solid var(--linea);text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}.nota{color:var(--tinta2)}code{font-size:13px}
@media (max-width:600px){section{padding:16px}}
"""
    doc = f"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>EDA Taco Bueno TLD</title><style>{css}</style></head>
<body><main><header><h1>Análisis exploratorio — Taco Bueno, ventas diarias TLD</h1>
<p>Generado el {datetime.now():%Y-%m-%d %H:%M} · carpeta: {html.escape(str(dir_datos))} · procesamiento: {segundos / 60:,.1f} min</p></header>
<div class="kpis">{''.join(f"<div class='kpi'><div class='v'>{v}</div><div class='l'>{k}</div></div>" for k, v in kpis)}</div>
{''.join(S)}</main></body></html>"""
    Path(ruta_html).write_text(doc, encoding="utf-8")


# --------------------------------------------------------------------------- #
# Programa principal
# --------------------------------------------------------------------------- #
def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    ap = argparse.ArgumentParser(description="EDA de los archivos diarios TLD de Taco Bueno.")
    ap.add_argument("--datos", default=str(Path(__file__).resolve().parent),
                    help="Carpeta con los CSV (por defecto, la carpeta del script).")
    ap.add_argument("--salida", default=None, help="Carpeta de salida (por defecto <datos>/eda_output).")
    ap.add_argument("--workers", type=int, default=max(1, min((os.cpu_count() or 2) - 1, 6)),
                    help="Procesos en paralelo (cada uno usa ~0.5 GB de RAM).")
    ap.add_argument("--muestra", type=int, default=0, help="Procesar solo N archivos espaciados (prueba rápida).")
    ap.add_argument("--sin-cache", action="store_true", help="No leer ni escribir el caché por archivo.")
    args = ap.parse_args()

    dir_datos = Path(args.datos)
    dir_salida = Path(args.salida) if args.salida else dir_datos / "eda_output"
    dir_tablas, dir_graf, dir_cache = dir_salida / "tablas", dir_salida / "graficas", dir_salida / "cache"
    for d in (dir_tablas, dir_graf) + (() if args.sin_cache else (dir_cache,)):
        d.mkdir(parents=True, exist_ok=True)

    archivos = sorted((p for p in dir_datos.glob("*.csv") if PATRON_ARCHIVO.search(p.name)),
                      key=lambda p: fecha_de_nombre(p.name))
    ignorados = [p.name for p in dir_datos.glob("*.csv") if not PATRON_ARCHIVO.search(p.name)]
    if not archivos:
        sys.exit(f"No se encontraron archivos TB_Daily_TLD_MM-DD-YYYY-m.csv en {dir_datos}")
    if args.muestra:
        idx = np.unique(np.linspace(0, len(archivos) - 1, args.muestra).round().astype(int))
        archivos = [archivos[i] for i in idx]
    total_gb = sum(p.stat().st_size for p in archivos) / 1e9
    print(f"Archivos a procesar: {len(archivos)} ({total_gb:,.1f} GB) con {args.workers} procesos")
    if ignorados:
        print(f"  CSV ignorados por no seguir el patrón de nombre: {len(ignorados)}")

    perfiles, perfil_cols = [], []
    acum = {k: Acumulador(v) for k, v in LLAVES.items()}
    t0 = time.time()
    desde_cache = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futuros = [ex.submit(procesar_archivo, str(p), None if args.sin_cache else str(dir_cache)) for p in archivos]
        for i, fut in enumerate(as_completed(futuros), 1):
            res = fut.result()
            desde_cache += bool(res.get("_desde_cache"))
            perfiles.append(res["perfil"])
            if len(res["columnas"]):
                perfil_cols.append(res["columnas"])
            for nombre, tabla in res["tablas"].items():
                acum[nombre].agregar(tabla)
            if "error" in res["perfil"]:
                print(f"  [error] {res['perfil']['archivo']}: {res['perfil']['error']}")
            if i % 25 == 0 or i == len(futuros):
                trans = time.time() - t0
                eta = trans / i * (len(futuros) - i)
                print(f"  {i:>4}/{len(futuros)} archivos · {trans / 60:5.1f} min · faltan ~{eta / 60:4.1f} min")
    seg = time.time() - t0
    print(f"Lectura terminada en {seg / 60:.1f} min ({desde_cache} desde caché). Construyendo tablas…")

    T = construir_tablas(perfiles, perfil_cols, acum)
    for nombre, df in T.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            comprimir = len(df) > 200_000
            df.to_csv(dir_tablas / (nombre + (".csv.gz" if comprimir else ".csv")), index=False,
                      encoding="utf-8-sig", float_format="%.4f")
    print(f"Tablas guardadas en {dir_tablas}")

    graficas = graficar(T, dir_graf)
    print(f"Gráficas guardadas en {dir_graf} ({len(graficas)})")

    ruta_html = dir_salida / "reporte_eda.html"
    generar_reporte(T, graficas, ruta_html, dir_datos, time.time() - t0)
    print(f"Reporte: {ruta_html}")

    print("\nHallazgos principales:")
    for x in hallazgos(T, T["inventario_archivos"]):
        print("  - " + re.sub(r"<[^>]+>", "", x))


if __name__ == "__main__":
    main()
