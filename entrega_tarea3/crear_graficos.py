import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

carpeta = Path.home() / "entrega_tarea3"
salida = carpeta / "graficos"
salida.mkdir(parents=True, exist_ok=True)

datos = pd.read_csv(carpeta / "resumen_mensual.csv")
datos["MES"] = pd.to_datetime(datos["MES"], format="%Y-%m")
datos = datos.sort_values("MES")

# Grafico 1: evolucion del promedio mensual
plt.figure(figsize=(12, 6))
plt.plot(datos["MES"], datos["promedio"], linewidth=1.5)
plt.title("Evolucion mensual del valor promedio")
plt.xlabel("Mes")
plt.ylabel("Valor promedio (COP)")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(salida / "promedio_mensual.png", dpi=150)
plt.close()

# Grafico 2: valores minimo y maximo por mes
plt.figure(figsize=(12, 6))
plt.plot(datos["MES"], datos["minimo"], label="Minimo", linewidth=1)
plt.plot(datos["MES"], datos["maximo"], label="Maximo", linewidth=1)
plt.title("Valores minimos y maximos mensuales")
plt.xlabel("Mes")
plt.ylabel("Valor (COP)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(salida / "minimos_maximos.png", dpi=150)
plt.close()

# Grafico 3: cantidad de registros por mes
plt.figure(figsize=(12, 6))
plt.plot(datos["MES"], datos["cantidad_registros"], linewidth=1)
plt.title("Cantidad de registros procesados por mes")
plt.xlabel("Mes")
plt.ylabel("Cantidad de registros")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(salida / "registros_mensuales.png", dpi=150)
plt.close()

print("Graficos creados correctamente:")
for archivo in sorted(salida.glob("*.png")):
    print(f"{archivo.name}: {archivo.stat().st_size:,} bytes")
