export const metadata = {
  title: 'BCE - Recherche Entreprises',
  description: 'Visualisation des données BCE',
}

export default function RootLayout({ children }) {
  return (
    <html lang="fr">
      <head>
        <style>{`
          * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
          }
          body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f5f5;
            color: #333;
          }
          .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
          }
        `}</style>
      </head>
      <body>
        <header style={{
          background: '#2563eb',
          color: 'white',
          padding: '20px 0',
          boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
        }}>
          <div className="container">
            <h1 style={{ fontSize: '28px', fontWeight: 'bold' }}>
              BCE - Base de données Entreprises
            </h1>
          </div>
        </header>
        {children}
      </body>
    </html>
  )
}