"use client";

import { useState } from "react";

export default function Home() {
  const [text, setText] = useState("");
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const handleAnalyze = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/analyze`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          text: text,
          n_min: 2,
          n_max: 6,
          threshold: 14.0,
          top_k: 50,
        }),
      });
      
      if (!res.ok) {
        let errorMsg = `Server svarte med status ${res.status}`;
        try {
          const errorData = await res.json();
          errorMsg = errorData.detail || errorMsg;
        } catch(e) {}
        throw new Error(errorMsg);
      }
      
      const data = await res.json();
      setResults(data.candidates || []);
    } catch (err: any) {
      console.error(err);
      alert(`Kunne ikke koble til API-et.\nFeilmelding: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 p-8 font-sans">
      <main className="max-w-4xl mx-auto bg-white p-8 rounded-2xl shadow-sm border border-gray-100">
        <h1 className="text-3xl font-bold mb-2 tracking-tight">Ibsen Allusjonsdetektor</h1>
        <p className="text-gray-500 mb-8">Lim inn en tekst for å finne potensielle allusjonsankre via informasjonstetthet.</p>

        <div className="mb-6">
          <label className="block text-sm font-medium text-gray-700 mb-2">Måltekst</label>
          <textarea
            className="w-full h-48 p-4 border border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all resize-none"
            placeholder="Lim inn tekst her..."
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
        </div>

        <button
          onClick={handleAnalyze}
          disabled={loading || !text}
          className="bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-xl transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {loading ? "Analyserer..." : "Finn Allusjonsankre (I-score)"}
        </button>

        {results.length > 0 && (
          <div className="mt-12">
            <h2 className="text-xl font-semibold mb-6 flex items-center gap-2">
              <span className="bg-blue-100 text-blue-800 py-1 px-3 rounded-full text-sm">{results.length}</span>
              Kandidater Funnet
            </h2>
            <div className="space-y-4">
              {results.map((item, idx) => (
                <div key={idx} className="p-5 border border-gray-100 bg-gray-50 rounded-xl">
                  <div className="flex justify-between items-start mb-2">
                    <h3 className="text-lg font-bold text-gray-900">"{item.phrase}"</h3>
                    <div className="flex gap-2">
                      <span className="text-xs font-semibold bg-indigo-100 text-indigo-800 px-2 py-1 rounded">I-score: {item.I_score}</span>
                      <span className="text-xs font-semibold bg-gray-200 text-gray-700 px-2 py-1 rounded">n={item.n}</span>
                    </div>
                  </div>
                  <p className="text-sm text-gray-600 mb-2 italic">"{item.sample_sentence}"</p>
                  <p className="text-xs text-gray-500">Forekomster: {item.occurrences_in_selection} | Bakgrunnsfrekvens: {item.background_count}</p>
                </div>
              ))}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
