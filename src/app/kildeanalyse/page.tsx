"use client";

import { useState, useMemo, useEffect, useRef } from "react";

type Candidate = {
  phrase: string;
  n: number;
  I_score: number;
  background_count: number;
  occurrences_in_selection: number;
  sample_sentence: string;
};

// Heat-map farger
function getHighlightColor(score: number) {
  if (score > 30) return "bg-red-200 text-red-900 border-red-300";
  if (score > 25) return "bg-orange-200 text-orange-900 border-orange-300";
  if (score > 20) return "bg-yellow-200 text-yellow-900 border-yellow-300";
  if (score > 15) return "bg-green-200 text-green-900 border-green-300";
  return "bg-blue-100 text-blue-900 border-blue-200";
}

function PoemHighlighter({ text, candidates, activeCandidate, onCandidateClick }: { text: string; candidates: Candidate[], activeCandidate: Candidate | null, onCandidateClick: (c: Candidate | null) => void }) {
  const parts = useMemo(() => {
    if (!candidates || candidates.length === 0 || !text) return [{ text, highlight: null }];

    type Match = { start: number; end: number; candidate: Candidate };
    const matches: Match[] = [];

    // Vi sorterer kandidatene fra lengst til kortest for å matche store fraser før små inni
    const sortedCandidates = [...candidates].sort((a, b) => b.phrase.length - a.phrase.length);

    sortedCandidates.forEach((cand) => {
      const escapedPhrase = cand.phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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

    matches.sort((a, b) => a.start - b.start || b.end - a.end);

    const validMatches: Match[] = [];
    let currentEnd = 0;
    
    for (const match of matches) {
      if (match.start >= currentEnd) {
        validMatches.push(match);
        currentEnd = match.end;
      }
    }

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
    <div className="whitespace-pre-wrap leading-loose font-serif text-lg text-gray-800" onClick={() => onCandidateClick(null)}>
      {parts.map((part, idx) => {
        if (!part.highlight) {
          return <span key={idx}>{part.text}</span>;
        }
        
        const c = part.highlight;
        const isActive = activeCandidate?.phrase === c.phrase;

        return (
          <span 
            key={idx} 
            id={`phrase-${c.phrase.replace(/\\s+/g, '-')}`}
            onClick={(e) => {
              e.stopPropagation();
              onCandidateClick(isActive ? null : c);
            }}
            className={`relative inline-block border-b-2 cursor-pointer transition-colors rounded-sm px-0.5 ${getHighlightColor(c.I_score)} ${isActive ? 'ring-2 ring-gray-900 z-20 font-bold' : 'opacity-90 hover:opacity-100'}`}
          >
            {part.text}
          </span>
        );
      })}
    </div>
  );
}

export default function Kildeanalyse() {
  const [poemText, setPoemText] = useState("");
  const [results, setResults] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeCandidate, setActiveCandidate] = useState<Candidate | null>(null);

  const [debugLogs, setDebugLogs] = useState<string[]>([]);
  const addLog = (msg: string) => setDebugLogs(prev => [...prev, msg]);
  const [workId, setWorkId] = useState("Terje Vigen");
  const [availableWorks, setAvailableWorks] = useState<string[]>([]);
  const [nMin, setNMin] = useState(2);
  const [nMax, setNMax] = useState(6);
  const [threshold, setThreshold] = useState(14.0);

  const listRef = useRef<HTMLDivElement>(null);

  const handleAnalyze = async () => {
    setLoading(true);
    setActiveCandidate(null);
    addLog(`Starter POST /api/analyze-source for workId: ${workId}...`);
    try {
      const res = await fetch(`/api/analyze-source`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          work_id: workId,
          n_min: nMin,
          n_max: nMax,
          threshold: threshold,
          top_k: 200,
        }),
      });
      
      const text = await res.text();
      addLog(`POST /api/analyze-source -> Status: ${res.status}. Body: ${text.slice(0, 150)}`);
      
      if (!res.ok) {
        throw new Error(`Status ${res.status}`);
      }
      
      const data = JSON.parse(text);
      setResults(data.candidates || []);
      setPoemText(data.text || "");
      addLog("Analyse fullført med suksess.");
    } catch (err: any) {
      console.error(err);
      addLog(`Feil i handleAnalyze: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    addLog("Henter /api/source-works...");
    fetch("/api/source-works").then(async res => {
      const text = await res.text();
      addLog(`GET /api/source-works -> Status: ${res.status}. Body: ${text.slice(0, 150)}`);
      try {
         const data = JSON.parse(text);
         setAvailableWorks(data.works || []);
      } catch (e) {
         addLog(`Feil ved parsing av /api/source-works: ${e}`);
      }
    }).catch(err => addLog(`Nettverksfeil /api/source-works: ${err}`));
  }, []);

  // Hent initielt
  useEffect(() => {
    handleAnalyze();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handlePoemPhraseClick = (c: Candidate | null) => {
    setActiveCandidate(c);
    if (c && listRef.current) {
      const el = document.getElementById(`list-item-${c.phrase.replace(/\\s+/g, '-')}`);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }
  };
  
  const handleListPhraseClick = (c: Candidate) => {
    setActiveCandidate(c);
    const el = document.getElementById(`phrase-${c.phrase.replace(/\\s+/g, '-')}`);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  };

  const handleElasticSearch = (e: React.MouseEvent, c: Candidate) => {
    e.stopPropagation();
    alert(`Her bygger vi Elastic-spørringen for Nettbiblioteket:\n\n{ "match_phrase": { "tekst": { "query": "${c.phrase}", "slop": 2 } } }\n\n(Denne vil skyte spørringen til elastic-clusteret senere.)`);
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 p-4 md:p-8 font-sans">
      <div className="max-w-7xl mx-auto">
        <header className="mb-8 flex justify-between items-end">
          <div>
            <h1 className="text-4xl font-extrabold tracking-tight text-gray-900">Kildeanalyse</h1>
            <p className="text-gray-500 mt-2 text-lg">Trekke ut allusjonsverdige fraser fra originalverk for Elastic-søk.</p>
          </div>
          <div className="flex gap-2">
                         <select value={workId} onChange={e => setWorkId(e.target.value)} className="p-2 border rounded-lg text-sm bg-white">
               {availableWorks.map(w => <option key={w} value={w}>{w}</option>)}
             </select>
             <button onClick={handleAnalyze} className="bg-gray-900 text-white px-4 py-2 rounded-lg text-sm font-bold">Oppdater</button>
          </div>
        </header>

        
        <div className="bg-gray-900 text-green-400 p-4 rounded-xl text-xs font-mono mb-8 overflow-y-auto max-h-48">
           <h3 className="text-white font-bold mb-2">Diagnostic Logs</h3>
           {debugLogs.map((l, i) => <div key={i}>{l}</div>)}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 h-[75vh]">
          {/* Venstre panel: Diktet */}
          <div className="bg-white p-6 md:p-8 rounded-2xl shadow-sm border border-gray-200 overflow-y-auto relative">
            <h2 className="text-2xl font-bold mb-6 sticky top-0 bg-white/90 backdrop-blur pb-2 z-10 border-b">{workId}</h2>
            {loading ? (
               <div className="flex justify-center items-center h-64 text-gray-400">Laster...</div>
            ) : poemText ? (
              <PoemHighlighter text={poemText} candidates={results} activeCandidate={activeCandidate} onCandidateClick={handlePoemPhraseClick} />
            ) : (
              <p className="text-gray-400">Ingen tekst funnet.</p>
            )}
          </div>

          {/* Høyre panel: Kandidat-listen */}
          <div className="bg-white p-4 rounded-2xl shadow-sm border border-gray-200 flex flex-col h-full overflow-hidden">
            <div className="p-4 border-b border-gray-100 flex justify-between items-center bg-gray-50 rounded-t-xl mb-2">
              <h2 className="text-lg font-bold text-gray-800">Gullkorn ({results.length})</h2>
              <span className="text-xs text-gray-500">Sortert på I-score</span>
            </div>
            
            <div className="overflow-y-auto flex-grow p-2" ref={listRef}>
              {results.map((c, idx) => (
                <div 
                  key={idx}
                  id={`list-item-${c.phrase.replace(/\\s+/g, '-')}`}
                  onClick={() => handleListPhraseClick(c)}
                  className={`p-4 mb-3 border rounded-xl cursor-pointer transition-all ${activeCandidate?.phrase === c.phrase ? 'ring-2 ring-gray-900 border-gray-900 bg-gray-50' : 'border-gray-200 hover:border-gray-300 hover:shadow-sm'}`}
                >
                  <div className="flex justify-between items-start mb-2">
                    <h3 className="font-bold text-lg leading-tight">{c.phrase}</h3>
                    <span className={`text-xs font-bold px-2 py-1 rounded ${getHighlightColor(c.I_score)}`}>
                      Score: {c.I_score.toFixed(1)}
                    </span>
                  </div>
                  <div className="flex justify-between items-end mt-4">
                     <div className="text-xs text-gray-500 space-x-3">
                        <span>Lengde: {c.n}</span>
                        <span>Frekvens: {c.background_count}</span>
                     </div>
                     <button 
                        onClick={(e) => handleElasticSearch(e, c)}
                        className="bg-blue-600 hover:bg-blue-700 text-white text-xs font-bold px-3 py-1.5 rounded-lg flex items-center gap-1 shadow-sm transition-colors"
                     >
                       <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                       Søk i Elastic
                     </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
