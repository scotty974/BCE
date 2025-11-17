export default function EntrepriseDetails({ data }) {
  const { entreprise, adresse, organes, activites, qualites, entreprises_absorbees } = data

  const Section = ({ title, children }) => (
    <div style={{
      background: 'white',
      padding: '24px',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      marginBottom: '20px'
    }}>
      <h3 style={{
        fontSize: '18px',
        fontWeight: '600',
        marginBottom: '16px',
        color: '#1f2937',
        borderBottom: '2px solid #e5e7eb',
        paddingBottom: '8px'
      }}>
        {title}
      </h3>
      {children}
    </div>
  )

  const Field = ({ label, value }) => value && (
    <div style={{ marginBottom: '12px' }}>
      <span style={{ fontWeight: '600', color: '#6b7280', marginRight: '8px' }}>
        {label}:
      </span>
      <span style={{ color: '#111827' }}>{value}</span>
    </div>
  )

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

      <h2 style={{
        fontSize: '32px',
        fontWeight: 'bold',
        marginBottom: '24px',
        color: '#111827'
      }}>
        {entreprise.nom}
      </h2>

      {/* Avertissement si entreprise remplacée ou clôturée */}
      {entreprise.statut && entreprise.statut !== 'Actif' && (
        <div style={{
          background: '#fef3c7',
          border: '1px solid #f59e0b',
          borderRadius: '8px',
          padding: '16px',
          marginBottom: '24px'
        }}>
          <div style={{ fontWeight: '600', color: '#92400e', marginBottom: '4px' }}>
            ⚠️ Attention
          </div>
          <div style={{ color: '#78350f' }}>
            Cette entreprise a le statut : <strong>{entreprise.statut}</strong>
          </div>
        </div>
      )}

      <Section title="Informations générales">
        <Field label="Numéro BCE" value={entreprise.numero} />
        <Field label="Statut" value={entreprise.statut} />
        <Field label="Type" value={entreprise.type} />
        <Field label="Forme légale" value={entreprise.forme_legale} />
        <Field label="Date de début" value={entreprise.date_debut} />
        <Field label="Capital" value={entreprise.capital} />
        <Field label="Nombre d'établissements" value={entreprise.nb_etablissements} />
      </Section>

      {adresse && (
        <Section title="Adresse du siège">
          <Field label="Rue" value={adresse.rue} />
          <Field label="Localité" value={adresse.code_postal_commune} />
          <Field label="Depuis" value={adresse.depuis} />
        </Section>
      )}

      {organes && organes.length > 0 && (
        <Section title={`Organes de gestion (${organes.length})`}>
          {organes.map((organe, idx) => (
            <div key={idx} style={{
              padding: '12px',
              background: '#f9fafb',
              borderRadius: '6px',
              marginBottom: '8px'
            }}>
              <div style={{ fontWeight: '600' }}>
                {organe.nom} {organe.prenom}
              </div>
              <div style={{ fontSize: '14px', color: '#6b7280' }}>
                {organe.fonction}
                {organe.depuis && ` • Depuis ${organe.depuis}`}
              </div>
            </div>
          ))}
        </Section>
      )}

      {activites && activites.length > 0 && (
        <Section title={`Activités (${activites.length})`}>
          {activites.map((activite, idx) => (
            <div key={idx} style={{
              padding: '12px',
              background: '#f9fafb',
              borderRadius: '6px',
              marginBottom: '8px'
            }}>
              <div style={{ fontWeight: '600' }}>
                Code {activite.code} ({activite.type} {activite.version})
              </div>
              <div style={{ fontSize: '14px', color: '#6b7280', marginTop: '4px' }}>
                {activite.description}
              </div>
            </div>
          ))}
        </Section>
      )}

      {qualites && qualites.length > 0 && (
        <Section title="Qualités">
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
            {qualites.map((qualite, idx) => (
              <span key={idx} style={{
                padding: '6px 12px',
                background: '#dbeafe',
                color: '#1e40af',
                borderRadius: '16px',
                fontSize: '14px',
                fontWeight: '500'
              }}>
                {qualite.type}
              </span>
            ))}
          </div>
        </Section>
      )}

      {entreprises_absorbees && entreprises_absorbees.length > 0 && (
        <Section title="Entreprises absorbées">
          {entreprises_absorbees.map((e, idx) => (
            <div key={idx} style={{
              padding: '12px',
              background: '#fef3c7',
              borderRadius: '6px',
              marginBottom: '8px'
            }}>
              <div style={{ fontWeight: '600' }}>{e.nom}</div>
              <div style={{ fontSize: '14px', color: '#92400e' }}>
                BCE: {e.numero}
              </div>
            </div>
          ))}
        </Section>
      )}
    </div>
  )
}