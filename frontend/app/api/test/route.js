import { NextResponse } from 'next/server'
import neo4j from 'neo4j-driver'

export async function GET() {
  let driver
  let session
  
  try {
    driver = neo4j.driver(
      process.env.NEO4J_URI || 'bolt://neo4j:7687',
      neo4j.auth.basic(
        process.env.NEO4J_USER || 'neo4j',
        process.env.NEO4J_PASSWORD || 'password'
      )
    )
    
    session = driver.session()
    
    // Test simple
    const result = await session.run('RETURN 1 as test')
    const count = await session.run('MATCH (e:Entreprise) RETURN count(e) as total')
    
    return NextResponse.json({
      status: 'OK',
      neo4j_connected: true,
      test_query: result.records[0].get('test'),
      entreprises_count: count.records[0].get('total').toNumber()
    })
  } catch (error) {
    console.error('Erreur Neo4j:', error)
    return NextResponse.json({
      status: 'ERROR',
      neo4j_connected: false,
      error: error.message,
      neo4j_uri: process.env.NEO4J_URI || 'bolt://neo4j:7687'
    }, { status: 500 })
  } finally {
    if (session) await session.close()
    if (driver) await driver.close()
  }
}