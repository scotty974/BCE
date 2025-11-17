'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'

// Fonction pour extraire et nettoyer le numéro
function cleanNumero(text) {
  const match = text.match(/0\d{3}[\.\s]?\d{3}[\.\s]?\d{3}/)
  if (match) {
    return match[0].replace(/\s/g, '')
  }
  return text
}

// Fonction pour valider qu'une chaîne est un numéro d'entreprise complet
function isValidEntityNumber(text) {
  const clean = text.replace(/[\.\s]/g, '')
  return /^0\d{9}$/.test(clean)
}

export default function SearchBar() {
  const [search, setSearch] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [scraping, setScraping] = useState(false)
  const [scrapeMessage, setScrapeMessage] = useState(null)
  const router = useRouter()

  // Recherche automatique quand on tape (debounce)
  useEffect(() => {
    if (!search.trim()) {
      setResults([])
      return
    }

    const timeoutId = setTimeout(async () => {
      setLoading(true)
      try {
        const response = await fetch(`/api/entreprises?search=${encodeURIComponent(search)}`)
        
        if (!response.ok) {
          console.error('Erreur API:', response.status, await response.text())
          setResults([])
          return
        }
        
        const data = await response.json()
        console.log('Résultats reçus:', data)
        setResults(data.entreprises || [])
      } catch (error) {
        console.error('Erreur de recherche:', error)
        setResults([])
      } finally {
        setLoading(false)
      }
    }, 500)

    return () => clearTimeout(timeoutId)
  }, [search])

  const handleSubmit = async (e) => {
    e.preventDefault()
  }

  const handleEntrepriseClick = (numero) => {
    const cleanedNumero = cleanNumero(numero)
    console.log('Navigation vers:', cleanedNumero)
    router.push(`/entreprise/${cleanedNumero}`)
  }

  const handleScrape = async () => {
    const entityNumber = cleanNumero(search)
    
    if (!isValidEntityNumber(entityNumber)) {
      setScrapeMessage({
        type: 'error',
        text: 'Veuillez entrer un numéro d\'entreprise valide (ex: 0203.430.576)'
      })
      return
    }
    
    setScraping(true)
    setScrapeMessage({
      type: 'info',
      text: 'Démarrage du scraping...'
    })
    
    try {
      const response = await fetch('/api/scrape', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ entity_number: entityNumber })
      })
      
      const data = await response.json()
      
      if (response.ok) {
        setScrapeMessage({
          type: 'success',
          text: `Scraping lancé ! L'entreprise ${entityNumber} sera disponible dans quelques minutes.`,
          dagRunId: data.dag_run_id
        })
        
        // Optionnel : polling pour vérifier le statut
        pollScrapingStatus(data.dag_run_id, entityNumber)
      } else {
        setScrapeMessage({
          type: 'error',
          text: data.error || 'Erreur lors du lancement du scraping'
        })
      }
    } catch (error) {
      console.error('Erreur scraping:', error)
      setScrapeMessage({
        type: 'error',
        text: 'Erreur de connexion au serveur'
      })
    } finally {
      setScraping(false)
    }
  }

  const pollScrapingStatus = async (dagRunId, entityNumber) => {
    const maxAttempts = 30 // 5 minutes max (30 * 10s)
    let attempts = 0
    
    const checkStatus = async () => {
      try {
        const response = await fetch(`/api/scrape/status/${dagRunId}`)
        const data = await response.json()
        
        if (data.state === 'success') {
          setScrapeMessage({
            type: 'success',
            text: `✅ Scraping terminé ! L'entreprise ${entityNumber} est maintenant disponible.`
          })
          
          // Rafraîchir la recherche
          setTimeout(() => {
            setSearch(entityNumber)
          }, 2000)
          
          return true
        } else if (data.state === 'failed') {
          setScrapeMessage({
            type: 'error',
            text: `❌ Le scraping a échoué. L'entreprise n'a peut-être pas été trouvée.`
          })
          return true
        } else if (data.state === 'running' || data.state === 'queued') {
          setScrapeMessage({
            type: 'info',
            text: `⏳ Scraping en cours... (${data.state})`
          })
        }
        
        return false
      } catch (error) {
        console.error('Erreur vérification statut:', error)
        return false
      }
    }
    
    const interval = setInterval(async () => {
      attempts++
      const finished = await checkStatus()
      
      if (finished || attempts >= maxAttempts) {
        clearInterval(interval)
        if (attempts >= maxAttempts) {
          setScrapeMessage({
            type: 'warning',
            text: `⏱️ Le scraping prend plus de temps que prévu. Vérifiez dans quelques minutes.`
          })
        }
      }
    }, 10000) // Vérifier toutes les 10 secondes
  }

  return (
    <div>
      <form onSubmit={handleSubmit} style={{ marginBottom: '20px' }}>
        <div style={{ display: 'flex', gap: '10px' }}>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Numéro BCE (ex: 0203.430.576) ou nom d'entreprise..."
            style={{
              flex: 1,
              padding: '12px 16px',
              border: '2px solid #e5e7eb',
              borderRadius: '6px',
              fontSize: '16px',
              outline: 'none',
              transition: 'border-color 0.2s'
            }}
            onFocus={(e) => e.target.style.borderColor = '#2563eb'}
            onBlur={(e) => e.target.style.borderColor = '#e5e7eb'}
          />
          <button
            type="submit"
            disabled={loading}
            style={{
              padding: '12px 32px',
              background: loading ? '#9ca3af' : '#2563eb',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              fontSize: '16px',
              fontWeight: '600',
              cursor: loading ? 'not-allowed' : 'pointer',
              transition: 'background 0.2s'
            }}
            onMouseEnter={(e) => !loading && (e.target.style.background = '#1d4ed8')}
            onMouseLeave={(e) => !loading && (e.target.style.background = '#2563eb')}
          >
            {loading ? 'Recherche...' : 'Rechercher'}
          </button>
        </div>
      </form>

      {/* Message de scraping */}
      {scrapeMessage && (
        <div style={{
          padding: '16px',
          borderRadius: '6px',
          marginBottom: '20px',
          background: scrapeMessage.type === 'success' ? '#dcfce7' : 
                     scrapeMessage.type === 'error' ? '#fee2e2' :
                     scrapeMessage.type === 'warning' ? '#fef3c7' : '#dbeafe',
          color: scrapeMessage.type === 'success' ? '#166534' :
                 scrapeMessage.type === 'error' ? '#991b1b' :
                 scrapeMessage.type === 'warning' ? '#92400e' : '#1e40af',
          border: `1px solid ${scrapeMessage.type === 'success' ? '#86efac' :
                              scrapeMessage.type === 'error' ? '#fca5a5' :
                              scrapeMessage.type === 'warning' ? '#fcd34d' : '#93c5fd'}`
        }}>
          {scrapeMessage.text}
        </div>
      )}

      {loading && (
        <div style={{
          padding: '20px',
          textAlign: 'center',
          color: '#6b7280',
          fontSize: '14px'
        }}>
          Recherche en cours...
        </div>
      )}

      {!loading && results.length > 0 && (
        <div style={{ marginTop: '20px' }}>
          <h3 style={{ fontSize: '18px', marginBottom: '15px', color: '#374151' }}>
            Résultats ({results.length})
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {results.map((entreprise) => (
              <div
                key={entreprise.numero}
                onClick={() => handleEntrepriseClick(entreprise.numero)}
                style={{
                  padding: '16px',
                  border: '1px solid #e5e7eb',
                  borderRadius: '6px',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  background: 'white'
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.borderColor = '#2563eb'
                  e.currentTarget.style.boxShadow = '0 4px 6px rgba(37, 99, 235, 0.1)'
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.borderColor = '#e5e7eb'
                  e.currentTarget.style.boxShadow = 'none'
                }}
              >
                <div style={{ fontWeight: '600', fontSize: '16px', marginBottom: '4px' }}>
                  {entreprise.nom}
                </div>
                <div style={{ fontSize: '14px', color: '#6b7280' }}>
                  BCE: {entreprise.numero_display || entreprise.numero}
                </div>
                {entreprise.statut && (
                  <div style={{
                    display: 'inline-block',
                    marginTop: '8px',
                    padding: '4px 12px',
                    background: entreprise.statut === 'Actif' ? '#dcfce7' : '#fee2e2',
                    color: entreprise.statut === 'Actif' ? '#166534' : '#991b1b',
                    borderRadius: '12px',
                    fontSize: '12px',
                    fontWeight: '600'
                  }}>
                    {entreprise.statut}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {!loading && results.length === 0 && search && isValidEntityNumber(search) && (
        <div style={{
          background: 'white',
          padding: '24px',
          borderRadius: '8px',
          border: '1px solid #e5e7eb',
          textAlign: 'center'
        }}>
          <div style={{ fontSize: '16px', color: '#6b7280', marginBottom: '16px' }}>
            Aucun résultat trouvé pour "{search}"
          </div>
          <div style={{ fontSize: '14px', color: '#9ca3af', marginBottom: '20px' }}>
            Cette entreprise n'est pas encore dans notre base de données.
          </div>
          <button
            onClick={handleScrape}
            disabled={scraping}
            style={{
              padding: '12px 24px',
              background: scraping ? '#9ca3af' : '#10b981',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              fontSize: '16px',
              fontWeight: '600',
              cursor: scraping ? 'not-allowed' : 'pointer',
              transition: 'background 0.2s'
            }}
            onMouseEnter={(e) => !scraping && (e.target.style.background = '#059669')}
            onMouseLeave={(e) => !scraping && (e.target.style.background = '#10b981')}
          >
            {scraping ? '🔄 Scraping en cours...' : '🔍 Scraper cette entreprise'}
          </button>
        </div>
      )}

      {!loading && results.length === 0 && search && !isValidEntityNumber(search) && (
        <div style={{
          padding: '20px',
          textAlign: 'center',
          color: '#6b7280',
          fontSize: '14px'
        }}>
          Aucun résultat trouvé pour "{search}"
        </div>
      )}
    </div>
  )
}