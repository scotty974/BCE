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

export async function GET(request, { params }) {
  const numero = decodeURIComponent(params.numero)
  
  console.log('[API] Détail entreprise demandé:', numero)
  
  const session = driver.session()

  try {
    // Supporter plusieurs formats
    const numeroClean = numero.replace(/[\.\s]/g, '')
    
    const result = await session.run(
      `
      MATCH (e:Entreprise)
      WHERE e.numero STARTS WITH $numero
         OR replace(replace(e.numero, '.', ''), ' ', '') STARTS WITH $numeroClean
         OR replace(replace(e.numero, '.', ''), ' ', '') = $numeroClean
      OPTIONAL MATCH (e)-[:SIEGE_A]->(a:Adresse)
      OPTIONAL MATCH (e)-[r:A_POUR_ORGANE]->(p:Personne)
      OPTIONAL MATCH (e)-[:EXERCE_ACTIVITE]->(act:Activite)
      OPTIONAL MATCH (e)-[:POSSEDE_QUALITE]->(q:Qualite)
      OPTIONAL MATCH (e)-[:ABSORBE]->(e2:Entreprise)
      RETURN e, a, 
             collect(DISTINCT {personne: p, fonction: r.fonction, depuis: r.depuis}) as organes,
             collect(DISTINCT act) as activites,
             collect(DISTINCT q) as qualites,
             collect(DISTINCT e2) as entreprises_absorbees
      LIMIT 1
      `,
      { numero, numeroClean }
    )

    console.log('[API] Nombre de résultats:', result.records.length)

    if (result.records.length === 0) {
      console.log('[API] Entreprise non trouvée pour:', numero)
      return NextResponse.json({ 
        error: 'Entreprise non trouvée',
        numero_recherche: numero 
      }, { status: 404 })
    }

    const record = result.records[0]
    
    const entreprise = convertNeo4jTypes(record.get('e').properties)
    const adresse = record.get('a') ? convertNeo4jTypes(record.get('a').properties) : null
    
    const organes = record.get('organes')
      .filter(o => o.personne)
      .map(o => convertNeo4jTypes({
        ...o.personne.properties,
        fonction: o.fonction,
        depuis: o.depuis
      }))
    
    const activites = record.get('activites')
      .map(a => convertNeo4jTypes(a.properties))
    
    const qualites = record.get('qualites')
      .map(q => convertNeo4jTypes(q.properties))
    
    const entreprises_absorbees = record.get('entreprises_absorbees')
      .map(e => convertNeo4jTypes(e.properties))

    return NextResponse.json({
      entreprise,
      adresse,
      organes,
      activites,
      qualites,
      entreprises_absorbees
    })
  } catch (error) {
    console.error('[API] Erreur Neo4j:', error)
    return NextResponse.json({ 
      error: 'Erreur serveur',
      details: error.message 
    }, { status: 500 })
  } finally {
    await session.close()
  }
}