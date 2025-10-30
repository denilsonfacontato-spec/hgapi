const express = require('express');
const fetch = require('node-fetch');
const { Pool } = require('pg');
const { Parser } = require('json2csv');

const app = express();
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const HG_KEY = 'd515df77';
const STALE_SECONDS = 432000; // 5 dias

// listas de códigos (substitua pelos reais)
const assets = {
  stocks: ['ITUB3','SANB11','WEGE3'],
  fiis: ['HGLG11','VISC11','KNRI11'],
  fiagro: ['FIA1','FIA2','FIA3']
};

// batch de 5 em 5
async function fetchBatch(type, codes) {
  const results = [];
  for (let i = 0; i < codes.length; i += 5) {
    const batch = codes.slice(i, i+5);
    const promises = batch.map(code => fetchAsset(type, code));
    const batchResults = await Promise.all(promises);
    results.push(...batchResults);
  }
  return results;
}

// busca com cache
async function fetchAsset(type, code) {
  const client = await pool.connect();
  try {
    const r = await client.query('SELECT data_json, updated_at FROM cache WHERE key=$1', [code]);
    if (r.rowCount) {
      const { data_json, updated_at } = r.rows[0];
      const age = (Date.now() - new Date(updated_at).getTime())/1000;
      if (age < STALE_SECONDS) return data_json;
    }
    const res = await fetch(`https://api.hgbrasil.com/finance/stock_price?key=${HG_KEY}&symbol=${code}`);
    const json = await res.json();
    await client.query(
      `INSERT INTO cache(key, data_json, updated_at) VALUES($1,$2,NOW())
       ON CONFLICT (key) DO UPDATE SET data_json = EXCLUDED.data_json, updated_at = NOW()`,
      [code, json]
    );
    return json;
  } finally { client.release(); }
}

// calcula intervalo Data Com - Data Pag
function addIntervals(data) {
  return data.map(item => {
    if(item.data_com && item.data_pag) {
      const interval = (new Date(item.data_pag) - new Date(item.data_com)) / (1000*60*60*24);
      return {...item, intervalo_dias: interval};
    }
    return item;
  });
}

// endpoints de lista
app.get('/stocks', (_req,res)=>res.json(assets.stocks));
app.get('/fiis', (_req,res)=>res.json(assets.fiis));
app.get('/fiagro', (_req,res)=>res.json(assets.fiagro));

// export CSV
app.get('/export/:type', async (req,res) => {
  const type = req.params.type;
  if(!assets[type]) return res.status(404).send('Tipo inválido');
  const codes = assets[type];
  const data = await fetchBatch(type, codes);
  const dataWithInterval = addIntervals(data);
  const parser = new Parser();
  const csv = parser.parse(dataWithInterval);
  res.header('Content-Type', 'text/csv');
  res.attachment(`${type}.csv`);
  res.send(csv);
});

// ping
app.get('/ping', (_req,res)=>res.send('pong'));

app.listen(process.env.PORT||3000, ()=>console.log('API up'));
