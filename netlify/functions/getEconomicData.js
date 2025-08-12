const fetch = require('node-fetch');

exports.handler = async function(event, context) {
    const FRED_API_KEY = process.env.FRED_API_KEY;
    const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

    // --- FRED API URLs ---
    const fredBaseUrl = `https://api.stlouisfed.org/fred/series/observations`;
    const commonFredParams = `api_key=${FRED_API_KEY}&file_type=json&sort_order=desc&limit=1`;
    
    const unemploymentUrl = `${fredBaseUrl}?series_id=UNRATE&${commonFredParams}`;
    // Correcto: Usar CPIAUCSL con units=pc1 para la inflación interanual
    const inflationUrl = `${fredBaseUrl}?series_id=CPIAUCSL&${commonFredParams}&units=pc1`;

    // --- Finnhub API URLs ---
    const today = new Date();
    const fromDate = today.toISOString().split('T')[0];
    const toDate = new Date(new Date().setDate(today.getDate() + 90)).toISOString().split('T')[0];
    const finnhubEcoUrl = `https://finnhub.io/api/v1/calendar/economic?from=${fromDate}&to=${toDate}&token=${FINNHUB_API_KEY}`;
    const finnhubEarningsUrl = `https://finnhub.io/api/v1/calendar/earnings?from=${fromDate}&to=${new Date(new Date().setDate(today.getDate() + 7)).toISOString().split('T')[0]}&token=${FINNHUB_API_KEY}`;

    try {
        const [unemploymentRes, inflationRes, finnhubEcoRes, finnhubEarningsRes] = await Promise.all([
            fetch(unemploymentUrl),
            fetch(inflationUrl),
            fetch(finnhubEcoUrl),
            fetch(finnhubEarningsUrl)
        ]);

        if (!unemploymentRes.ok || !inflationRes.ok || !finnhubEcoRes.ok || !finnhubEarningsRes.ok) {
            throw new Error('API network response was not ok');
        }

        const unemploymentData = await unemploymentRes.json();
        const inflationData = await inflationRes.json();
        const finnhubEcoData = await finnhubEcoRes.json();
        const finnhubEarningsData = await finnhubEarningsRes.json();

        return {
            statusCode: 200,
            body: JSON.stringify({
                unemploymentData,
                inflationData,
                finnhubEcoData,
                finnhubEarningsData
            })
        };

    } catch (error) {
        console.error("Error in serverless function:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: "Failed to fetch economic data." })
        };
    }
};
