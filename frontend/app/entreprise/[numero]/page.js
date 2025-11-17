'use client'

import { useState, useEffect } from 'react'
import EntrepriseDetails from '@/components/EntrepriseDetails'

// Fonction pour extraire uniquement le numéro d'entreprise
function extractNumero(rawNumero) {
  // Extraire le premier pattern qui ressemble à un numéro d'entreprise
  // Format: 0XXX.XXX.XXX ou 0XXXXXXXXX
  const match = rawNumero.match(/0\d{3}[\.\s]?\d{3}[\.\s]?\d{3}/)
  if (match) {
    return match[0]
  }
  // Si pas de match, retourner tel quel
  return rawNumero
}

export default function EntreprisePage({ params }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Nettoyer le numéro
  const cleanNumero = extractNumero(decodeURIComponent(params.numero))

  useEffect(() => {
    async function fetchData() {
      try {
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

  return <EntrepriseDetails data={data} />
}