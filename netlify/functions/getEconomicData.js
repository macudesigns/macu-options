exports.handler = async function(event, context) {
    const FRED_API_KEY    = process.env.FRED_API_KEY;
    const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
    const fredBase        = 'https://api.stlouisfed.org/fred/series/observations';
    const finnBase        = 'https://finnhub.io/api/v1';

    async function safeFetch(url, fallback, extraHeaders = {}) {
        try {
            const res = await fetch(url, {
                signal: AbortSignal.timeout(7000),
                headers: { 'User-Agent': 'Mozilla/5.0', ...extraHeaders },
            });
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


    const SECTORS   = ['XLK','XLC','XLF','XLV','XLY','XLP','XLI','XLE','XLU','XLRE','XLB'];
    const WATCHLIST = ['AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','AVGO','AMD',
                       'JPM','BAC','WMT','XOM','LLY','GLD','USO','URA','GDX','PLTR','QQQ'];
    const emptyObs  = { observations: [{ value: 'N/A', date: '-' }] };
    const emptyObs0 = { observations: [] };
    const todayStr  = new Date().toISOString().split('T')[0];
    const to90Str   = new Date(Date.now() + 90 * 86400000).toISOString().split('T')[0];
    const to7Str    = new Date(Date.now() +  7 * 86400000).toISOString().split('T')[0];

    // ONE parallel phase: FRED + Finnhub metrics/quotes + calendars
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
        // FRED SP500 daily series for SPY chart (1)
        safeFetch(fredUrl('SP500', 200),          emptyObs0),
        // Finnhub calendars (2)
        safeFetch(finnBase + '/calendar/economic?from=' + todayStr + '&to=' + to90Str + '&token=' + FINNHUB_API_KEY, { economicCalendar: [] }),
        safeFetch(finnBase + '/calendar/earnings?from=' + todayStr + '&to=' + to7Str  + '&token=' + FINNHUB_API_KEY, { earningsCalendar: [] }),
        // Finnhub sector metrics (11) + sector quotes (11)
        ...SECTORS.map(sym => safeFetch(finnBase + '/stock/metric?symbol=' + sym + '&metric=all&token=' + FINNHUB_API_KEY, {})),
        ...SECTORS.map(sym => safeFetch(finnBase + '/quote?symbol=' + sym + '&token=' + FINNHUB_API_KEY, {})),
        // Finnhub watchlist metrics (20) for volume flow
        ...WATCHLIST.map(sym => safeFetch(finnBase + '/stock/metric?symbol=' + sym + '&metric=all&token=' + FINNHUB_API_KEY, {})),
    ]);

    const [unemployment, inflation, fedfunds,
           walcl, wtregen, rrp,
           nfci, hySpread, t10y2y, vixFred, dxy,
           sp500Fred,
           finnhubEcoData, earningsRaw,
           ...marketResults] = allResults;

    const sectorMetrics = marketResults.slice(0, SECTORS.length);
    const sectorQuotes  = marketResults.slice(SECTORS.length, SECTORS.length * 2);
    const watchMetrics  = marketResults.slice(SECTORS.length * 2);

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

    function pctRank(arr, val) {
        if (val == null) return 50;
        const sorted = arr.filter(v => v != null).sort((a, b) => a - b);
        if (sorted.length < 2) return 50;
        return (sorted.filter(v => v < val).length / (sorted.length - 1)) * 100;
    }

    // ── Score sectors using Finnhub pre-computed metrics ─────────────────
    const rawSectors = SECTORS.map((sym, i) => {
        const m = (sectorMetrics[i] || {}).metric;
        if (!m) return null;
        const perf1m     = m.monthToDatePriceReturnDaily           ?? null;
        const perf3m     = m['13WeekPriceReturnDaily']              ?? null;
        const perf5d     = m['5DayPriceReturnDaily']                ?? null;
        const relVsSpy3m = m['priceRelativeToS&P50013Week']         ?? null;
        const vol10d     = m['10DayAverageTradingVolume']           ?? null;
        const vol3mo     = m['3MonthAverageTradingVolume']          ?? null;
        const volTrend   = (vol3mo != null && vol3mo > 0) ? vol10d / vol3mo : null;
        const latest     = (sectorQuotes[i] || {}).c               ?? null;
        if (perf3m == null) return null;
        return { sym, latest, perf1m, perf3m, perf5d, relVsSpy3m, volTrend };
    }).filter(Boolean);

    if (rawSectors.length > 0) {
        const F = {
            relVsSpy3m: rawSectors.map(s => s.relVsSpy3m),
            perf1m:     rawSectors.map(s => s.perf1m),
            perf3m:     rawSectors.map(s => s.perf3m),
            perf5d:     rawSectors.map(s => s.perf5d),
            volTrend:   rawSectors.map(s => s.volTrend),
        };
        var scoredSectors = rawSectors.map(s => ({
            sym: s.sym, latest: s.latest, perf1m: s.perf1m, perf3m: s.perf3m,
            relVsSpy3m: s.relVsSpy3m, volTrend: s.volTrend,
            rsScore:  Math.round(pctRank(F.relVsSpy3m, s.relVsSpy3m) * 0.40 + pctRank(F.perf1m, s.perf1m) * 0.35 + pctRank(F.volTrend, s.volTrend) * 0.25),
            oppScore: Math.round((100 - pctRank(F.perf3m, s.perf3m)) * 0.40 + pctRank(F.volTrend, s.volTrend) * 0.30 + pctRank(F.perf5d, s.perf5d) * 0.30),
        }));
    } else {
        var scoredSectors = [];
    }
    const byRS  = [...scoredSectors].sort((a, b) => b.rsScore  - a.rsScore);
    const byOpp = [...scoredSectors].sort((a, b) => b.oppScore - a.oppScore);

    // ── Volume flows using Finnhub pre-computed volume averages ──────────
    const flowAssets = WATCHLIST.map((sym, i) => {
        const m = (watchMetrics[i] || {}).metric;
        if (!m) return null;
        const vol10d = m['10DayAverageTradingVolume']  ?? null;
        const vol3mo = m['3MonthAverageTradingVolume'] ?? null;
        if (vol10d == null || vol3mo == null || vol3mo === 0) return null;
        const volRatio = Math.round((vol10d / vol3mo) * 100) / 100;
        const priceChg = Math.round((m['5DayPriceReturnDaily'] ?? 0) * 100) / 100;
        return { sym, volRatio, priceChg };
    }).filter(Boolean);
    const flowingIn = flowAssets.filter(a => a.volRatio > 1.3 && a.priceChg > 0).sort((a, b) => b.volRatio - a.volRatio).slice(0, 8);
    const dryingUp  = flowAssets.filter(a => a.volRatio < 0.7).sort((a, b) => a.volRatio - b.volRatio).slice(0, 8);

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

    // SPY chart from FRED S&P 500 daily series
    const spyForChart = (sp500Fred.observations || [])
        .filter(o => o.value !== '.' && o.value !== 'N/A')
        .map(o => ({ date: o.date, value: parseFloat(o.value) }))
        .reverse()
        .slice(-120);

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
