# 🤖 Bot EMA Crossover — Binance → Telegram (Deploy en Render)

Detecta cruces de 3 EMAs en todos los pares USDT de Binance cada minuto
y envía alertas a Telegram. Desplegado gratis en **Render.com**.

---

## 📁 Archivos del proyecto

```
ema_alert_bot/
├── bot.py            ← Código principal del bot
├── requirements.txt  ← Dependencias Python
├── render.yaml       ← Configuración automática de Render
└── README.md
```

---

## PASO 1 — Crear el Bot de Telegram

1. Abre Telegram → busca **@BotFather** → escribe `/newbot`
2. Elige un nombre y un username para tu bot
3. BotFather te dará el **TOKEN** → guárdalo
   ```
   Ejemplo: 7123456789:AAF-abc123XYZ...
   ```
4. Inicia una conversación con tu bot (búscalo y presiona Start)

5. Obtén tu **Chat ID**:
   - Busca **@userinfobot** en Telegram → `/start`
   - Te mostrará tu ID numérico
   ```
   Ejemplo: 987654321
   ```

---

## PASO 2 — Subir el código a GitHub

1. Crea una cuenta en **github.com** (gratis)
2. Crea un nuevo repositorio (ej: `ema-bot`)
3. Sube los 4 archivos:

```bash
git init
git add .
git commit -m "EMA Bot Binance"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/ema-bot.git
git push -u origin main
```

> Si no tienes Git, puedes arrastrar los archivos directo en la interfaz web de GitHub.

---

## PASO 3 — Desplegar en Render (GRATIS)

### 3.1 Crear cuenta
- Ve a **https://render.com**
- Regístrate con tu cuenta de **GitHub** (gratis, sin tarjeta de crédito)

### 3.2 Crear el Web Service
1. Dashboard → **New +** → **Web Service**
2. Conecta tu repositorio de GitHub
3. Render detectará automáticamente el `render.yaml` ✅

### 3.3 Configurar manualmente (si no usa render.yaml)

| Campo | Valor |
|---|---|
| **Name** | ema-alert-bot |
| **Runtime** | Python 3 |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `python bot.py` |
| **Plan** | Free |

### 3.4 Agregar variables de entorno
En Render → tu servicio → **Environment** → agrega:

| Variable | Valor |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Tu token de BotFather |
| `TELEGRAM_CHAT_ID` | Tu Chat ID numérico |
| `EMA_FAST` | `8` |
| `EMA_MID` | `21` |
| `EMA_SLOW` | `55` |

### 3.5 Deploy
- Haz clic en **Create Web Service**
- Render desplegará automáticamente 🚀
- Verás los logs en tiempo real

---

## PASO 4 — Evitar que Render duerma el servicio (IMPORTANTE)

El plan gratuito de Render pausa el servicio tras 15 minutos sin peticiones HTTP.
El bot ya incluye un servidor HTTP interno, pero necesitas un "ping" externo.

### Solución gratis con UptimeRobot:
1. Ve a **https://uptimerobot.com** → regístrate gratis
2. **Add New Monitor**:
   - Monitor Type: **HTTP(s)**
   - URL: `https://TU-BOT.onrender.com/health`
   - Monitoring Interval: **5 minutos**
3. ¡Listo! UptimeRobot hará ping cada 5 min y el bot nunca dormirá ✅

---

## 🔔 Cómo se ve la alerta en Telegram

```
🟢 CRUCE 3 EMAs — ALCISTA ▲
━━━━━━━━━━━━━━━━━━━━━━
📊 Par:       BTCUSDT
💰 Precio:   $67,234.500000
📉 Cambio 1m: +0.12%
━━━━━━━━━━━━━━━━━━━━━━
📈 EMA 8:   67,198.432100
📈 EMA 21:  67,150.217800
📈 EMA 55:  67,089.654300
━━━━━━━━━━━━━━━━━━━━━━
⏱ Timeframe: 1 Minuto
🕐 2024-01-15 14:32 UTC
```

---

## ⚙️ Personalizar las EMAs

Cambia los valores en Render → Environment:

| Setup | EMA_FAST | EMA_MID | EMA_SLOW |
|---|---|---|---|
| Agresivo | 5 | 13 | 34 |
| **Balanceado** (default) | **8** | **21** | **55** |
| Conservador | 10 | 50 | 200 |

Después de cambiar variables, Render hace redeploy automático.

---

## 📊 Dashboard de estado del bot

El bot expone una página de estado en:
```
https://TU-BOT.onrender.com
```
Muestra: EMAs configuradas, símbolos monitoreados, último escaneo y total de alertas.
