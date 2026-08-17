"use client";

import { useState, useMemo } from "react";

type Candidate = {
  phrase: string;
  n: number;
  I_score: number;
  background_count: number;
  occurrences_in_selection: number;
  sample_sentence: string;
  ibsen_match?: string | null;
};

// Enkel funksjon for å bygge et farge-heat-map basert på I-score
function getHighlightColor(score: number) {
  if (score > 30) return "bg-red-200 text-red-900 border-red-300";
  if (score > 25) return "bg-orange-200 text-orange-900 border-orange-300";
  if (score > 20) return "bg-yellow-200 text-yellow-900 border-yellow-300";
  if (score > 15) return "bg-green-200 text-green-900 border-green-300";
  return "bg-blue-100 text-blue-900 border-blue-200";
}

function TextHighlighter({ text, candidates }: { text: string; candidates: Candidate[] }) {
  const parts = useMemo(() => {
    if (!candidates || candidates.length === 0 || !text) return [{ text, highlight: null }];

    type Match = { start: number; end: number; candidate: Candidate };
    const matches: Match[] = [];

    candidates.forEach((cand) => {
      // Escape special regex chars
      const escapedPhrase = cand.phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      // Case-insensitive, word boundaries
      const regex = new RegExp(`\\b${escapedPhrase}\\b`, "gi");
      
      let match;
      while ((match = regex.exec(text)) !== null) {
        matches.push({
          start: match.index,
          end: match.index + match[0].length,
          candidate: cand,
        });
      }
    });

    // Sorter på start-indeks, og ved lik start: la den lengste (mest spesifikke) vinne
    matches.sort((a, b) => a.start - b.start || b.end - a.end);

    // Fjern overlappende matcher (grådig tilnærming fra venstre)
    const validMatches: Match[] = [];
    let currentEnd = 0;
    
    for (const match of matches) {
      if (match.start >= currentEnd) {
        validMatches.push(match);
        currentEnd = match.end;
      }
    }

    // Bygg opp tekstdelene (vanlig tekst og markert tekst)
    const result = [];
    let lastIndex = 0;
    
    for (const match of validMatches) {
      if (match.start > lastIndex) {
        result.push({ text: text.slice(lastIndex, match.start), highlight: null });
      }
      result.push({ text: text.slice(match.start, match.end), highlight: match.candidate });
      lastIndex = match.end;
    }
    
    if (lastIndex < text.length) {
      result.push({ text: text.slice(lastIndex), highlight: null });
    }
    
    return result;
  }, [text, candidates]);

  return (
    <div className="whitespace-pre-wrap leading-loose font-serif text-lg text-gray-800">
      {parts.map((part, idx) => {
        if (!part.highlight) {
          return <span key={idx}>{part.text}</span>;
        }
        
        const c = part.highlight;
        return (
          <span 
            key={idx} 
            className={`relative group inline-block border-b-2 cursor-help transition-colors rounded-sm px-0.5 ${getHighlightColor(c.I_score)}`}
          >
            {part.text}
            
            {/* Tooltip / Popover */}
            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-80 p-4 bg-gray-900 text-white text-sm rounded-lg shadow-xl opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-10 pointer-events-none">
              <div className="font-bold border-b border-gray-700 pb-2 mb-3 flex justify-between items-center">
                <span className="text-base">{c.phrase}</span>
                <span className="text-blue-300 font-mono bg-gray-800 px-2 py-1 rounded">I-score: {c.I_score.toFixed(1)}</span>
              </div>
              
              {c.ibsen_match ? (
                <div className="mb-3 bg-gray-800 rounded p-2 border border-gray-700">
                  <span className="block text-[10px] text-gray-400 uppercase tracking-wider mb-1 font-semibold">Ibsen match</span>
                  <div className="text-gray-200 text-xs italic font-serif leading-relaxed">
                    "{c.ibsen_match}"
                  </div>
                </div>
              ) : (
                <div className="mb-3 text-gray-400 text-xs italic">
                  Ingen direkte treff i Ibsen-tekster.
                </div>
              )}

              <div className="flex justify-between text-[11px] text-gray-400 mt-2">
                <span>Lengde: {c.n} ord</span>
                <span>Frekvens: {c.background_count}</span>
              </div>
              {/* Lille pilen nederst */}
              <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
            </div>
          </span>
        );
      })}
    </div>
  );
}

export default function Home() {
  const [text, setText] = useState("");
  const [analyzedText, setAnalyzedText] = useState("");
  const [results, setResults] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(false);

  // Innstillinger
  const [nMin, setNMin] = useState(2);
  const [nMax, setNMax] = useState(6);
  const [threshold, setThreshold] = useState(14.0);
  const [topK, setTopK] = useState(100);

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
          n_min: nMin,
          n_max: nMax,
          threshold: threshold,
          top_k: topK,
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
      setAnalyzedText(text);
    } catch (err: any) {
      console.error(err);
      alert(`Kunne ikke koble til API-et.\nFeilmelding: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 p-4 md:p-8 font-sans">
      <div className="max-w-7xl mx-auto">
        <header className="mb-8">
          <h1 className="text-4xl font-extrabold tracking-tight text-gray-900">Ibsen Allusjonsdetektor</h1>
          <p className="text-gray-500 mt-2 text-lg">Analyser tekster for å finne potensielle allusjoner til Henrik Ibsen (basert på informasjonstetthet).</p>
        </header>

        <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
          {/* Venstre kolonne: Input og innstillinger */}
          <div className="lg:col-span-5 space-y-6">
            <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-200">
              <label className="block text-sm font-semibold text-gray-700 mb-2">Måltekst</label>
              <textarea
                className="w-full h-64 p-4 border border-gray-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all resize-none text-gray-800"
                placeholder="Lim inn tekst her..."
                value={text}
                onChange={(e) => setText(e.target.value)}
              />
              
              <div className="mt-6 grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">Min lengde (ord)</label>
                  <input type="number" value={nMin} onChange={e => setNMin(Number(e.target.value))} className="w-full p-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">Maks lengde (ord)</label>
                  <input type="number" value={nMax} onChange={e => setNMax(Number(e.target.value))} className="w-full p-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">I-score terskel</label>
                  <input type="number" step="0.5" value={threshold} onChange={e => setThreshold(Number(e.target.value))} className="w-full p-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">Maks antall treff</label>
                  <input type="number" value={topK} onChange={e => setTopK(Number(e.target.value))} className="w-full p-2 border border-gray-200 rounded-lg text-sm" />
                </div>
              </div>

              <button
                onClick={handleAnalyze}
                disabled={loading || !text}
                className="w-full mt-6 bg-gray-900 hover:bg-gray-800 text-white font-semibold py-3 px-6 rounded-xl transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-md flex justify-center items-center gap-2"
              >
                {loading ? (
                  <svg className="animate-spin h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                ) : "Marker allusjoner"}
              </button>
            </div>
          </div>

          {/* Høyre kolonne: Resultater / Highlighter */}
          <div className="lg:col-span-7">
            <div className="bg-white p-6 md:p-8 rounded-2xl shadow-sm border border-gray-200 min-h-[500px]">
              {analyzedText ? (
                <div>
                  <div className="flex justify-between items-end mb-6 border-b border-gray-100 pb-4">
                    <h2 className="text-xl font-bold text-gray-800">Analyse</h2>
                    <span className="bg-blue-50 text-blue-700 text-xs font-bold px-3 py-1 rounded-full border border-blue-100">
                      Fant {results.length} unike kandidater
                    </span>
                  </div>
                  <TextHighlighter text={analyzedText} candidates={results} />
                </div>
              ) : (
                <div className="h-full flex flex-col items-center justify-center text-gray-400">
                  <svg className="w-16 h-16 mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>
                  <p>Ferdig analysert tekst vil dukke opp her med interaktive markeringer.</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
