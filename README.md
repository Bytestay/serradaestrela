
# bytestay/serradaestrela — Config por ficheiro (LIGHT/FULL)

## Como usar
- **Produção** (workflow usa este por defeito):
  ```bash
  python mini_casafari.py --config config_full.yml
  ```
- **Teste rápido**:
  ```bash
  python mini_casafari.py --config config_light.yml
  ```

## O que podes ajustar (sem tocar no workflow)
Ver `config_full.yml` e `config_light.yml`:
- limites de preço, T2+
- retries/timeout
- ciclos e pausas
- *per-source limit* por portal
- rotação de prioridade
- keywords
- fontes e ordem
- caminho do ficheiro de localidades (`localities_file`)
- pasta de saída (`out_dir`) e prefixo (`out_prefix`)

## Dica
Mantém **ambos** os ficheiros e muda só `config_full.yml` quando precisares alterar produção.
