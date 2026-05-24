const fetch = require('node-fetch');

exports.handler = async function(event, context) {
    const FRED_API_KEY    = process.env.FRED_API_KEY;
    const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

    const fredBaseUrl      = `https://api.stlouisfed.org/fred/series/observations`;
    const commonFredParams = `api_key=${FRED_API_KEY}&file_type=json&sort_order=desc&limit=1`;
    const unemploymentUrl  = `${fredBaseUrl}?series_id=UNRATE&${commonFredParams}`;
    const inflationUrl     = `${fredBaseUrl}?series_id=CPIAUCSL&${commonFredParams}&units=pc1`;
    const fedfundsUrl      = `${fredBaseUrl}?series_id=FEDFUNDS&${commonFredParams}`;

    const today   = new Date();
    const from    = today.toISOString().split('T')[0];
    const to90    = new Date(today.getTime() + 90 * 86400000).toISOString().split('T')[0];
    const to7     = new Date(today.getTime() +  7 * 86400000).toISOString().split('T')[0];
    const finnhubEcoUrl      = `https://finnhub.io/api/v1/calendar/economic?from=${from}&to=${to90}&token=${FINNHUB_API_KEY}`;
    const finnhubEarningsUrl = `https://finnhub.io/api/v1/calendar/earnings?from=${from}&to=${to7}&token=${FINNHUB_API_KEY}`;

    // Fetch each source independently so one failure doesn't break the rest
    async function safeFetch(url, fallback) {
        try {
            const res = await fetch(url);
            if (!res.ok) { console.warn(`safeFetch non-OK ${res.status}: ${url}`); return fallback; }
            return await res.json();
        } catch(e) {
            console.warn(`safeFetch error for ${url}:`, e.message);
            return fallback;
        }
    }

    const [unemploymentData, inflationData, fedfundsData, finnhubEcoData, finnhubEarningsRaw] = await Promise.all([
        safeFetch(unemploymentUrl,    { observations: [{ value: 'N/A', date: '—' }] }),
        safeFetch(inflationUrl,       { observations: [{ value: 'N/A', date: '—' }] }),
        safeFetch(fedfundsUrl,        { observations: [{ value: 'N/A', date: '—' }] }),
        safeFetch(finnhubEcoUrl,      { economicCalendar: [] }),
        safeFetch(finnhubEarningsUrl, { earningsCalendar: [] })
    ]);

    // Enrich earnings with company names via /search (broader coverage than profile2)
    const earningsList = (finnhubEarningsRaw.earningsCalendar || []).slice(0, 15);
    const symbols = [...new Set(earningsList.map(e => e.symbol))].slice(0, 12);
    const searches = await Promise.all(
        symbols.map(sym =>
            safeFetch(
                `https://finnhub.io/api/v1/search?q=${encodeURIComponent(sym)}&token=${FINNHUB_API_KEY}`,
                { count: 0, result: [] }
            )
        )
    );
    const nameMap = {};
    searches.forEach((res, i) => {
        const sym = symbols[i];
        const match = (res.result || []).find(r => r.symbol === sym || r.displaySymbol === sym);
        nameMap[sym] = match ? match.description : '';
    });
    const enrichedEarnings = earningsList.map(e => ({ ...e, companyName: nameMap[e.symbol] || '' }));
    const finnhubEarningsData = { earningsCalendar: enrichedEarnings };

    return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ unemploymentData, inflationData, fedfundsData, finnhubEcoData, finnhubEarningsData })
    };
};

