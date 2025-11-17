'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'

// Fonction pour extraire et nettoyer le numéro
function cleanNumero(text) {
  // Extraire le premier pattern qui ressemble à un numéro d'entreprise
  const match = text.match(/0\d{3}[\.\s]?\d{3}[\.\s]?\d{3}/)
  if (match) {
    // Retourner sans les espaces mais garder les points
    return match[0].replace(/\s/g, '')
  }
  return text
}

export default function SearchBar() {
  const [search, setSearch] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
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
    // Nettoyer le numéro avant la navigation
    const cleanedNumero = cleanNumero(numero)
    console.log('Navigation vers:', cleanedNumero)
    router.push(`/entreprise/${cleanedNumero}`)
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

      {!loading && results.length === 0 && search && (
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