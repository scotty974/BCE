'use client'

import { useState } from 'react'

export default function RescrapeButton({ entityNumber }) {
  const [scraping, setScraping] = useState(false)
  const [message, setMessage] = useState(null)
  const [dagRunId, setDagRunId] = useState(null)

  const handleRescrape = async () => {
    setScraping(true)
    setMessage({
      type: 'info',
      text: 'Démarrage du re-scraping...'
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
        setDagRunId(data.dag_run_id)
        setMessage({
          type: 'success',
          text: `Re-scraping lancé ! Les données seront mises à jour dans quelques instants.`
        })
        
        // Commencer le polling
        pollScrapingStatus(data.dag_run_id)
      } else {
        setMessage({
          type: 'error',
          text: data.error || 'Erreur lors du lancement du re-scraping'
        })
        setScraping(false)
      }
    } catch (error) {
      console.error('Erreur re-scraping:', error)
      setMessage({
        type: 'error',
        text: 'Erreur de connexion au serveur'
      })
      setScraping(false)
    }
  }

  const pollScrapingStatus = async (dagRunId) => {
    const maxAttempts = 60
    let attempts = 0
    
    const checkStatus = async () => {
      try {
        const response = await fetch(`/api/scrape/status/${dagRunId}`)
        const data = await response.json()
        
        if (data.state === 'success') {
          setMessage({
            type: 'success',
            text: `✅ Re-scraping terminé ! Rechargement de la page...`
          })
          
          // Recharger la page après 2 secondes
          setTimeout(() => {
            window.location.reload()
          }, 2000)
          
          return true
        } else if (data.state === 'failed') {
          setMessage({
            type: 'error',
            text: `❌ Le re-scraping a échoué. Veuillez réessayer.`
          })
          setScraping(false)
          return true
        } else if (data.state === 'running' || data.state === 'queued') {
          setMessage({
            type: 'info',
            text: `⏳ Re-scraping en cours... (${Math.floor(attempts * 10 / 60)} min ${(attempts * 10) % 60} sec)`
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
        if (attempts >= maxAttempts && !finished) {
          setMessage({
            type: 'warning',
            text: `⏱️ Le re-scraping prend plus de temps que prévu.`
          })
          setScraping(false)
        }
      }
    }, 10000)
  }

  return (
    <div style={{
      background: 'white',
      padding: '20px',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      marginBottom: '24px'
    }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '16px',
        flexWrap: 'wrap'
      }}>
        <div>
          <h3 style={{ fontSize: '16px', fontWeight: '600', marginBottom: '4px' }}>
            Mettre à jour les données
          </h3>
          <p style={{ fontSize: '14px', color: '#6b7280' }}>
            Récupérer les dernières informations depuis la BCE
          </p>
        </div>
        <button
          onClick={handleRescrape}
          disabled={scraping}
          style={{
            padding: '10px 20px',
            background: scraping ? '#9ca3af' : '#2563eb',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            fontSize: '14px',
            fontWeight: '600',
            cursor: scraping ? 'not-allowed' : 'pointer',
            transition: 'background 0.2s',
            display: 'flex',
            alignItems: 'center',
            gap: '8px'
          }}
          onMouseEnter={(e) => !scraping && (e.target.style.background = '#1d4ed8')}
          onMouseLeave={(e) => !scraping && (e.target.style.background = '#2563eb')}
        >
          <span>{scraping ? '🔄' : '↻'}</span>
          {scraping ? 'Re-scraping...' : 'Actualiser'}
        </button>
      </div>

      {message && (
        <div style={{
          marginTop: '16px',
          padding: '12px',
          borderRadius: '6px',
          background: message.type === 'success' ? '#dcfce7' : 
                     message.type === 'error' ? '#fee2e2' :
                     message.type === 'warning' ? '#fef3c7' : '#dbeafe',
          color: message.type === 'success' ? '#166534' :
                 message.type === 'error' ? '#991b1b' :
                 message.type === 'warning' ? '#92400e' : '#1e40af',
          border: `1px solid ${message.type === 'success' ? '#86efac' :
                              message.type === 'error' ? '#fca5a5' :
                              message.type === 'warning' ? '#fcd34d' : '#93c5fd'}`,
          fontSize: '14px'
        }}>
          {message.text}
        </div>
      )}
    </div>
  )
}