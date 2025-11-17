'use client'

import SearchBar from '@/components/SearchBar'

export default function Home() {
  return (
    <main className="container" style={{ paddingTop: '40px' }}>
      <div style={{
        background: 'white',
        padding: '40px',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        marginBottom: '40px'
      }}>
        <h2 style={{ fontSize: '24px', marginBottom: '20px' }}>
          Rechercher une entreprise
        </h2>
        <SearchBar />
      </div>

      <div style={{
        background: 'white',
        padding: '30px',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
      }}>
        <h3 style={{ fontSize: '20px', marginBottom: '15px' }}>
          Comment utiliser
        </h3>
        <ul style={{ lineHeight: '1.8', paddingLeft: '20px' }}>
          <li>Recherchez une entreprise par son numéro BCE (ex: 0203.430.576)</li>
          <li>Recherchez par nom d'entreprise</li>
          <li>Cliquez sur une entreprise pour voir tous les détails</li>
        </ul>
      </div>
    </main>
  )
}