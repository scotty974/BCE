import { NextResponse } from 'next/server'
import neo4j from 'neo4j-driver'

const driver = neo4j.driver(
  process.env.NEO4J_URI || 'bolt://neo4j:7687',
  neo4j.auth.basic(
    process.env.NEO4J_USER || 'neo4j',
    process.env.NEO4J_PASSWORD || 'password'
  )
)

// Fonction pour convertir les valeurs Neo4j en valeurs JavaScript
function convertNeo4jTypes(obj) {
  if (obj === null || obj === undefined) return obj
  
  if (neo4j.isInt(obj)) {
    return obj.toNumber()
  }
  
  if (Array.isArray(obj)) {
    return obj.map(convertNeo4jTypes)
  }
  
  if (typeof obj === 'object') {
    const converted = {}
    for (const [key, value] of Object.entries(obj)) {
      converted[key] = convertNeo4jTypes(value)
    }
    return converted
  }
  
  return obj
}

// Fonction pour extraire le numéro propre
function extractNumero(text) {
  const match = text.match(/0\d{3}[\.\s]?\d{3}[\.\s]?\d{3}/)
  if (match) {
    return match[0].replace(/\s/g, '')
  }
  return text
}

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const search = searchParams.get('search')

  console.log('[API] Recherche reçue:', search)

  if (!search) {
    return NextResponse.json({ entreprises: [] })
  }

  const session = driver.session()

  try {
    const searchClean = search.replace(/[\.\s]/g, '')
    
    const result = await session.run(
      `
      MATCH (e:Entreprise)
      WHERE e.numero CONTAINS $search 
         OR e.numero STARTS WITH $search
         OR replace(replace(e.numero, '.', ''), ' ', '') CONTAINS $searchClean
         OR toLower(e.nom) CONTAINS toLower($search)
      RETURN e.numero as numero_brut, 
             e.nom as nom, 
             e.statut as statut
      LIMIT 20
      `,
      { search, searchClean }
    )

    console.log('[API] Nombre de résultats:', result.records.length)

    const entreprises = result.records.map(record => {
      const numeroBrut = convertNeo4jTypes(record.get('numero_brut'))
      const numeroClean = extractNumero(numeroBrut)
      
      return {
        numero: numeroClean,  // Numéro nettoyé pour l'URL
        numero_display: numeroBrut,  // Numéro original pour l'affichage
        nom: convertNeo4jTypes(record.get('nom')),
        statut: convertNeo4jTypes(record.get('statut'))
      }
    })

    return NextResponse.json({ entreprises })
  } catch (error) {
    console.error('[API] Erreur Neo4j:', error)
    return NextResponse.json({ 
      error: 'Erreur de recherche',
      details: error.message 
    }, { status: 500 })
  } finally {
    await session.close()
  }
}