'use client'

import { useState, useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
import EntrepriseDetails from '@/components/EntrepriseDetails'
import RescrapeButton from '@/components/RescrapeButton'

// Fonction pour extraire uniquement le numéro d'entreprise
function extractNumero(segments) {
  const fullPath = Array.isArray(segments) ? segments.join('/') : segments
  const match = fullPath.match(/0\d{3}[\.\s]?\d{3}[\.\s]?\d{3}/)
  if (match) {
    return match[0].replace(/\s/g, '')
  }
  return Array.isArray(segments) ? segments[0] : segments || ''
}

export default function EntreprisePage({ params }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const searchParams = useSearchParams()
  const showScrapedMessage = searchParams.get('scraped') === 'true'

  const cleanNumero = extractNumero(params.numero)

  useEffect(() => {
    async function fetchData() {
      try {
        console.log('Fetching entreprise:', cleanNumero)
        const res = await fetch(`/api/entreprises/${encodeURIComponent(cleanNumero)}`)
        
        if (!res.ok) {
          throw new Error(`Erreur: ${res.status}`)
        }
        
        const json = await res.json()
        setData(json)
      } catch (err) {
        console.error('Erreur de chargement:', err)
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [cleanNumero])

  if (loading) {
    return (
      <div className="container" style={{ paddingTop: '40px' }}>
        <div style={{
          background: 'white',
          padding: '40px',
          borderRadius: '8px',
          textAlign: 'center'
        }}>
          <div style={{ fontSize: '18px', color: '#6b7280' }}>
            Chargement...
          </div>
        </div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="container" style={{ paddingTop: '40px' }}>
        <div style={{
          background: 'white',
          padding: '40px',
          borderRadius: '8px',
          textAlign: 'center'
        }}>
          <h2 style={{ fontSize: '24px', color: '#ef4444', marginBottom: '10px' }}>
            Entreprise non trouvée
          </h2>
          <p style={{ color: '#6b7280', marginBottom: '8px' }}>
            {error || 'Aucune donnée disponible'}
          </p>
          <p style={{ fontSize: '14px', color: '#9ca3af' }}>
            Numéro recherché : {cleanNumero}
          </p>
          <a
            href="/"
            style={{
              display: 'inline-block',
              marginTop: '20px',
              color: '#2563eb',
              textDecoration: 'none',
              fontWeight: '500'
            }}
          >
            ← Retour à la recherche
          </a>
        </div>
      </div>
    )
  }

  return (
    <div className="container" style={{ paddingTop: '40px' }}>
      <a
        href="/"
        style={{
          display: 'inline-block',
          marginBottom: '20px',
          color: '#2563eb',
          textDecoration: 'none',
          fontWeight: '500'
        }}
      >
        ← Retour à la recherche
      </a>

      {/* Message de succès du scraping initial */}
      {showScrapedMessage && (
        <div style={{
          background: '#dcfce7',
          border: '2px solid #86efac',
          borderRadius: '8px',
          padding: '16px 20px',
          marginBottom: '24px',
          display: 'flex',
          alignItems: 'center',
          gap: '12px'
        }}>
          <span style={{ fontSize: '24px' }}>🎉</span>
          <div>
            <div style={{ fontWeight: '600', color: '#166534', marginBottom: '4px' }}>
              Scraping réussi !
            </div>
            <div style={{ fontSize: '14px', color: '#15803d' }}>
              L'entreprise {cleanNumero} a été ajoutée avec succès à la base de données.
            </div>
          </div>
        </div>
      )}

      {/* Bouton de re-scraping */}
      <RescrapeButton entityNumber={cleanNumero} />
      
      <EntrepriseDetails data={data} />
    </div>
  )
}