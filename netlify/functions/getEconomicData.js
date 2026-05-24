const fetch = require('node-fetch');

exports.handler = async function(event, context) {
    const FRED_API_KEY    = process.env.FRED_API_KEY;
    const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
    const fredBase        = 'https://api.stlouisfed.org/fred/series/observations';
    const finnBase        = 'https://finnhub.io/api/v1';

    async function safeFetch(url, fallback) {
        try {
            const res = await fetch(url, { timeout: 8000 });
            if (!res.ok) { console.warn('safeFetch non-OK', res.status, url); return fallback; }
            return await res.json();
        } catch(e) {
            console.warn('safeFetch error:', e.message, url);
            return fallback;
        }
    }

    function fredUrl(series, limit, units) {
        let u = fredBase + '?series_id=' + series + '&api_key=' + FRED_API_KEY + '&file_type=json&sort_order=desc&limit=' + (limit || 1);
        if (units) u += '&units=' + units;
        return u;
    }

    function finnhubCandle(symbol, days) {
        const to   = Math.floor(Date.now() / 1000);
        const from = to - (days || 220) * 86400;
        return finnBase + '/stock/candle?symbol=' + symbol + '&resolution=D&from=' + from + '&to=' + to + '&token=' + FINNHUB_API_KEY;
    }

    const SECTORS   = ['XLK','XLC','XLF','XLV','XLY','XLP','XLI','XLE','XLU','XLRE','XLB'];
    const WATCHLIST = ['AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','AVGO','AMD',
                       'JPM','BAC','WMT','XOM','LLY','GLD','USO','URA','GDX','PLTR','QQQ'];
    const emptyObs    = { observations: [{ value: 'N/A', date: '-' }] };
    const emptyObs0   = { observations: [] };
    const emptyCandle = { s: 'no_data' };
    const todayStr    = new Date().toISOString().split('T')[0];
    const to90Str     = new Date(Date.now() + 90 * 86400000).toISOString().split('T')[0];
    const to7Str      = new Date(Date.now() +  7 * 86400000).toISOString().split('T')[0];

    // ONE parallel phase: FRED + all candles + calendars
    const allSymbols = ['SPY', ...SECTORS, ...WATCHLIST];
    const allResults = await Promise.all([
        // FRED macro (11)
        safeFetch(fredUrl('UNRATE'),              emptyObs),
        safeFetch(fredUrl('CPIAUCSL', 1, 'pc1'),  emptyObs),
        safeFetch(fredUrl('FEDFUNDS'),            emptyObs),
        safeFetch(fredUrl('WALCL',      260),     emptyObs0),
        safeFetch(fredUrl('WTREGEN',    260),     emptyObs0),
        safeFetch(fredUrl('RRPONTSYD',  260),     emptyObs0),
        safeFetch(fredUrl('NFCI',         5),     emptyObs0),
        safeFetch(fredUrl('BAMLH0A0HYM2', 5),     emptyObs0),
        safeFetch(fredUrl('T10Y2Y',       5),     emptyObs0),
        safeFetch(fredUrl('VIXCLS',       5),     emptyObs0),
        safeFetch(fredUrl('DTWEXBGS',     5),     emptyObs0),
        // Finnhub calendars (2)
        safeFetch(finnBase + '/calendar/economic?from=' + todayStr + '&to=' + to90Str + '&token=' + FINNHUB_API_KEY, { economicCalendar: [] }),
        safeFetch(finnBase + '/calendar/earnings?from=' + todayStr + '&to=' + to7Str  + '&token=' + FINNHUB_API_KEY, { earningsCalendar: [] }),
        // Candles: SPY + 11 sectors + 20 watchlist (32)
        ...allSymbols.map(sym => safeFetch(finnhubCandle(sym), emptyCandle)),
    ]);

    const [unemployment, inflation, fedfunds,
           walcl, wtregen, rrp,
           nfci, hySpread, t10y2y, vixFred, dxy,
           finnhubEcoData, earningsRaw,
           ...candleResults] = allResults;

    const spyCandle     = candleResults[0];
    const sectorCandles = candleResults.slice(1, 1 + SECTORS.length);
    const watchCandles  = candleResults.slice(1 + SECTORS.length);

    // Earnings enrichment (depends on earningsRaw — sequential but fast)
    const earningsList = (earningsRaw.earningsCalendar || []).slice(0, 15);
    const uniqueSyms   = [...new Set(earningsList.map(e => e.symbol))].slice(0, 10);
    const searches     = await Promise.all(
        uniqueSyms.map(sym => safeFetch(finnBase + '/search?q=' + encodeURIComponent(sym) + '&token=' + FINNHUB_API_KEY, { result: [] }))
    );
    const nameMap = {};
    searches.forEach((res, i) => {
        const sym   = uniqueSyms[i];
        const match = (res.result || []).find(r => r.symbol === sym || r.displaySymbol === sym);
        nameMap[sym] = match ? match.description : '';
    });
    const finnhubEarningsData = { earningsCalendar: earningsList.map(e => ({ ...e, companyName: nameMap[e.symbol] || '' })) };

    // ── Candle metric extractor ──────────────────────────────────────────
    function computeMetrics(c) {
        if (!c || c.s !== 'ok' || !c.c || c.c.length < 22) return null;
        const cl = c.c, vl = c.v, n = cl.length;
        const latest = cl[n - 1];
        const d1m    = cl[n - 22];
        const d3m    = n >= 64  ? cl[n - 64]  : null;
        const d5     = n >= 6   ? cl[n - 6]   : null;
        const sma50  = n >= 50  ? cl.slice(n - 50).reduce((a, b) => a + b, 0) / 50   : null;
        const sma200 = n >= 200 ? cl.slice(n - 200).reduce((a, b) => a + b, 0) / 200 : null;
        const vol5d  = vl.slice(n - 5).reduce((a, b) => a + b, 0) / 5;
        const vol20d = vl.slice(n - 20).reduce((a, b) => a + b, 0) / 20;
        const vol3d  = vl.slice(n - 3).reduce((a, b) => a + b, 0) / 3;
        const vol30d = n >= 30 ? vl.slice(n - 30).reduce((a, b) => a + b, 0) / 30 : vol20d;
        return {
            latest,
            perf1m:   ((latest - d1m) / d1m) * 100,
            perf3m:   d3m  != null ? ((latest - d3m) / d3m) * 100 : null,
            perf5d:   d5   != null ? ((latest - d5)  / d5)  * 100 : null,
            dist50:   sma50  ? ((latest - sma50)  / sma50)  * 100 : null,
            dist200:  sma200 ? ((latest - sma200) / sma200) * 100 : null,
            volTrend: vol20d > 0 ? vol5d / vol20d : null,
            volSpike: vol30d > 0 ? vol3d / vol30d : null,
        };
    }

    function pctRank(arr, val) {
        if (val == null) return 50;
        const sorted = arr.filter(v => v != null).sort((a, b) => a - b);
        if (sorted.length < 2) return 50;
        return (sorted.filter(v => v < val).length / (sorted.length - 1)) * 100;
    }

    // ── Score sectors ────────────────────────────────────────────────────
    const spyM       = computeMetrics(spyCandle);
    const rawSectors = SECTORS.map((sym, i) => {
        const m = computeMetrics(sectorCandles[i]);
        if (!m) return null;
        return { sym, ...m, relVsSpy3m: (m.perf3m != null && spyM && spyM.perf3m != null) ? m.perf3m - spyM.perf3m : null };
    }).filter(Boolean);

    if (rawSectors.length > 0) {
        const F = { relVsSpy3m: rawSectors.map(s => s.relVsSpy3m), perf1m: rawSectors.map(s => s.perf1m),
                    perf3m: rawSectors.map(s => s.perf3m), perf5d: rawSectors.map(s => s.perf5d),
                    volTrend: rawSectors.map(s => s.volTrend), volSpike: rawSectors.map(s => s.volSpike),
                    dist50: rawSectors.map(s => s.dist50), dist200: rawSectors.map(s => s.dist200) };
        var scoredSectors = rawSectors.map(s => ({
            sym: s.sym, latest: s.latest, perf1m: s.perf1m, perf3m: s.perf3m,
            relVsSpy3m: s.relVsSpy3m, volTrend: s.volTrend,
            rsScore:  Math.round(pctRank(F.relVsSpy3m, s.relVsSpy3m) * 0.35 + pctRank(F.perf1m, s.perf1m) * 0.25 + pctRank(F.volTrend, s.volTrend) * 0.20 + pctRank(F.dist50, s.dist50) * 0.10 + pctRank(F.dist200, s.dist200) * 0.10),
            oppScore: Math.round((100 - pctRank(F.perf3m, s.perf3m)) * 0.40 + pctRank(F.volSpike, s.volSpike) * 0.30 + pctRank(F.perf5d, s.perf5d) * 0.30),
        }));
    } else {
        var scoredSectors = [];
    }
    const byRS  = [...scoredSectors].sort((a, b) => b.rsScore  - a.rsScore);
    const byOpp = [...scoredSectors].sort((a, b) => b.oppScore - a.oppScore);

    // ── Volume flows ─────────────────────────────────────────────────────
    const flowAssets = WATCHLIST.map((sym, i) => {
        const c = watchCandles[i];
        if (!c || c.s !== 'ok' || !c.c || c.c.length < 21) return null;
        const n = c.c.length, vl = c.v, cl = c.c;
        const vol5d  = vl.slice(n - 5).reduce((a, b) => a + b, 0) / 5;
        const vol20d = vl.slice(n - 20).reduce((a, b) => a + b, 0) / 20;
        const ratio  = vol20d > 0 ? vol5d / vol20d : 1;
        const chg    = cl.length >= 2 ? ((cl[n-1] - cl[n-2]) / cl[n-2]) * 100 : 0;
        return { sym, volRatio: Math.round(ratio * 100) / 100, priceChg: Math.round(chg * 100) / 100, price: cl[n-1] };
    }).filter(Boolean);
    const flowingIn = flowAssets.filter(a => a.volRatio > 1.5 && a.priceChg > 0).sort((a, b) => b.volRatio - a.volRatio).slice(0, 8);
    const dryingUp  = flowAssets.filter(a => a.volRatio < 0.6).sort((a, b) => a.volRatio - b.volRatio).slice(0, 8);

    // ── Net Liquidity (units: billions USD) ──────────────────────────────
    // WALCL = millions, WTREGEN = millions, RRPONTSYD = billions
    function parseObs(obs, toB) {
        return (obs || []).filter(o => o.value !== '.' && o.value !== 'N/A')
            .map(o => ({ date: o.date, val: parseFloat(o.value) * (toB ? 0.001 : 1) })).reverse();
    }
    function forwardFill(series, targetDate) {
        for (let i = series.length - 1; i >= 0; i--) { if (series[i].date <= targetDate) return series[i].val; }
        return series.length ? series[0].val : 0;
    }
    // Convert WALCL and WTREGEN from millions to billions (divide by 1000); RRPONTSYD already in billions
    const walclS = parseObs(walcl.observations,   true);
    const tgaS   = parseObs(wtregen.observations, true);
    const rrpS   = parseObs(rrp.observations,     false);
    const netLiquidity = walclS.slice(-120).map(w => ({
        date:  w.date,
        value: Math.round((w.val - forwardFill(tgaS, w.date) - forwardFill(rrpS, w.date)) * 10) / 10,
    }));

    const spyForChart = spyCandle && spyCandle.s === 'ok'
        ? spyCandle.t.map((t, i) => ({ date: new Date(t * 1000).toISOString().split('T')[0], value: spyCandle.c[i] })).slice(-120)
        : [];

    // ── Macro single values ───────────────────────────────────────────────
    function latestVal(obs) {
        const valid = (obs || []).filter(o => o.value !== '.' && o.value !== 'N/A');
        return valid.length ? parseFloat(valid[0].value) : null;
    }
    const hySpreadVal = latestVal(hySpread.observations);
    const nfciVal     = latestVal(nfci.observations);
    const macro = {
        nfci:       nfciVal,
        hySpread:   hySpreadVal,
        t10y2y:     latestVal(t10y2y.observations),
        vix:        latestVal(vixFred.observations),
        dxy:        latestVal(dxy.observations),
        bunkerMode: (hySpreadVal != null && hySpreadVal > 4.5) || (nfciVal != null && nfciVal > 0.5),
    };

    return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=3600' },
        body: JSON.stringify({
            unemploymentData: unemployment,
            inflationData:    inflation,
            fedfundsData:     fedfunds,
            finnhubEcoData,
            finnhubEarningsData,
            macro,
            netLiquidity,
            spyForChart,
            sectorLeaders:  byRS.slice(0, 3),
            sectorLaggards: byRS.slice(-3).reverse(),
            sectorOpps:     byOpp.slice(0, 3),
            sectorAll:      byRS,
            flowingIn,
            dryingUp,
            generatedAt: new Date().toISOString(),
            v: 2,
        }),
    };
};
