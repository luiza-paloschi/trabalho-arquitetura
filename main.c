#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/time.h>

#define ARQUIVO_TXT "jewelry_ordenado.csv"
#define ARQUIVO_JOIAS "joias.bin"
#define ARQUIVO_COMPRAS "compras.bin"
#define ARQUIVO_INDICE_JOIAS "indice_joias.bin"
#define ARQUIVO_INDICE_COMPRAS "indice_compras.bin"

#define TAMANHO_INTERVALO_JOIAS 100
#define TAMANHO_INTERVALO_COMPRAS 500
#define MAX_INSERCOES 100

typedef struct header_joias {
    int num_registros;
    int num_excluidos;
    int num_insercoes;
} header_joias;

typedef struct registroJoia {
    uint64_t id;
    int id_marca;
    float preco;
    char genero;
    char cor_principal[10];
    char metal_principal[10];
    char joia_principal[10];
    short excluido;
} registro_joia;

typedef struct resultado_pesquisa {
    int encontrado;
    long posicao;
    registro_joia joia;
} resultado_pesquisa;

typedef struct registroCompra {
    uint64_t id;
    uint64_t id_produto;
    int quantidade;
    char data_hora[24];
    uint64_t id_usuario;
} registro_compra;

typedef struct entrada_indice_joia{
    uint64_t chave;
    long posicao;
} entrada_indice_joia;

typedef struct indice_joias {
    int num_entradas;
    entrada_indice_joia *entradas;
} indice_joias;

typedef struct entrada_indice_compra {
    uint64_t id_pedido;
    uint64_t id_produto;
    long posicao;
} entrada_indice_compra;

typedef struct indice_compras {
    int num_entradas;
    entrada_indice_compra *entradas;
} indice_compras;

//////////////////////////////////////////////////////
/////ÍNDICES
///////////////////////////////////////////////////////

void liberar_indice_joias(indice_joias *indice) {
    if (indice->entradas) {
        free(indice->entradas);
        indice->entradas = NULL;
        indice->num_entradas = 0;
    }
}

void liberar_indice_compras(indice_compras *indice) {
    if (indice->entradas) {
        free(indice->entradas);
        indice->entradas = NULL;
        indice->num_entradas = 0;
    }
}

void criar_indice_joias(indice_joias *indice) {
    FILE *arq_joias = fopen(ARQUIVO_JOIAS, "rb");
    if (arq_joias == NULL) {
        printf("Erro ao abrir arquivo de produtos para criar índice\n");
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_joias);
    int num_produtos = header.num_registros;

    int num_entradas = (num_produtos + TAMANHO_INTERVALO_JOIAS - 1) / TAMANHO_INTERVALO_JOIAS;
    indice->entradas = malloc(num_entradas * sizeof(entrada_indice_joia));
    if (!indice->entradas) {
        printf("Erro de alocação de memória para índice\n");
        fclose(arq_joias);
        return;
    }

    indice->num_entradas = num_entradas;

    registro_joia produto;
    int intervalo_atual = 0;
    int i;

    for (i = 0; i < num_produtos; i++) {
        long posicao_atual = ftell(arq_joias);

        if (fread(&produto, sizeof(registro_joia), 1, arq_joias)) {
            if (i % TAMANHO_INTERVALO_JOIAS == 0) {
                indice->entradas[intervalo_atual].chave = produto.id;
                indice->entradas[intervalo_atual].posicao = posicao_atual;
                intervalo_atual++;
            }
        }
    }

    fclose(arq_joias);
    printf("Índice de produtos criado com %d entradas\n", indice->num_entradas);
}

void criar_indice_compras(indice_compras *indice) {
    FILE *arq_compras = fopen(ARQUIVO_COMPRAS, "rb");
    if (!arq_compras) {
        printf("Erro ao abrir arquivo de compras para criar índice\n");
        return;
    }

    fseek(arq_compras, 0, SEEK_END);
    long tamanho_arquivo = ftell(arq_compras);
    int num_compras = tamanho_arquivo / sizeof(registro_compra);
    rewind(arq_compras);

    int num_entradas = (num_compras + TAMANHO_INTERVALO_COMPRAS - 1) / TAMANHO_INTERVALO_COMPRAS;
    indice->entradas = malloc(num_entradas * sizeof(entrada_indice_compra));
    indice->num_entradas = num_entradas;

    registro_compra compra;
    int intervalo_atual = 0, i;

    for (i = 0; i < num_compras; i++) {
        long posicao_atual = ftell(arq_compras);

        if (fread(&compra, sizeof(registro_compra), 1, arq_compras)) {
            if (i % TAMANHO_INTERVALO_COMPRAS == 0) {
                indice->entradas[intervalo_atual].id_pedido = compra.id;
                indice->entradas[intervalo_atual].id_produto = compra.id_produto;
                indice->entradas[intervalo_atual].posicao = posicao_atual;
                intervalo_atual++;
            }
        }
    }

    fclose(arq_compras);
    printf("Índice de compras criado com %d entradas\n", indice->num_entradas);
}

int carregar_indice_joias(indice_joias *indice) {
    FILE *arq_indice = fopen(ARQUIVO_INDICE_JOIAS, "rb");
    if (arq_indice == NULL) {
        return 0;
    }

    fread(&indice->num_entradas, sizeof(int), 1, arq_indice);
    indice->entradas = malloc(indice->num_entradas * sizeof(entrada_indice_joia));
    fread(indice->entradas, sizeof(entrada_indice_joia), indice->num_entradas, arq_indice);

    fclose(arq_indice);
    printf("Índice de joias carregado com %d entradas\n", indice->num_entradas);
    return 1;
}

int carregar_indice_compras(indice_compras *indice) {
    FILE *arq_indice = fopen(ARQUIVO_INDICE_COMPRAS, "rb");
    if (!arq_indice) {
        return 0;
    }

    fread(&indice->num_entradas, sizeof(int), 1, arq_indice);
    indice->entradas = malloc(indice->num_entradas * sizeof(entrada_indice_compra));
    fread(indice->entradas, sizeof(entrada_indice_compra), indice->num_entradas, arq_indice);

    fclose(arq_indice);
    printf("Índice de compras carregado com %d entradas\n", indice->num_entradas);
    return 1;
}

void salvar_indices(indice_joias *indice_j, indice_compras *indice_c) {
    FILE *arq_indice_j = fopen(ARQUIVO_INDICE_JOIAS, "wb");
    if (!arq_indice_j) {
        printf("Erro ao salvar índice de joias\n");
        return;
    }

    fwrite(&indice_j->num_entradas, sizeof(int), 1, arq_indice_j);
    fwrite(indice_j->entradas, sizeof(entrada_indice_joia), indice_j->num_entradas, arq_indice_j);

    fclose(arq_indice_j);
    printf("Índice de produtos salvo com %d entradas\n", indice_j->num_entradas);

    FILE *arq_indice_c = fopen(ARQUIVO_INDICE_COMPRAS, "wb");
    if (!arq_indice_c) {
        printf("Erro ao salvar índice de compras\n");
        return;
    }

    fwrite(&indice_c->num_entradas, sizeof(int), 1, arq_indice_c);
    fwrite(indice_c->entradas, sizeof(entrada_indice_compra), indice_c->num_entradas, arq_indice_c);

    fclose(arq_indice_c);
    printf("Índice de compras salvo com %d entradas\n", indice_c->num_entradas);
}

////////////////////////////////////////////////////////////////
////////CRIAÇÃO DOS ARQUIVOS DE DADOS
////////////////////////////////////////////////////////////////

void preencher_campo_vazio(char *dest, const char *src, int tamanho) {
    if (src == NULL || strlen(src) == 0) {
        memset(dest, ' ', tamanho - 1);
        dest[tamanho - 1] = '\0';
    } else {
        strncpy(dest, src, tamanho - 1);
        dest[tamanho - 1] = '\0';
    }
}

int produto_existe(registro_joia *joias, int count, const char *id_produto) {
    uint64_t id_produto_ull = strtoull(id_produto, NULL, 10);

    int i;
    for (i = 0; i < count; i++) {
        if (joias[i].id == id_produto_ull) {
            return 1;
        }
    }
    return 0;
}

int splitar_linha(char *linha, char **campos, int max_campos) {
    int count = 0;
    char *inicio = linha;
    char *ptr = linha;

    while (*ptr != '\0' && count < max_campos) {
        if (*ptr == ';') {
            *ptr = '\0';
            campos[count++] = inicio;
            inicio = ptr + 1;
        }
        ptr++;
    }

    if (count < max_campos) {
        campos[count++] = inicio;
    }

    return count;
}

int comparar_joias(const void *a, const void *b) {
    const registro_joia *joiaA = (const registro_joia *)a;
    const registro_joia *joiaB = (const registro_joia *)b;
    if (joiaA->id < joiaB->id) return -1;
    if (joiaA->id > joiaB->id) return 1;
    return 0;
}

void criar_arquivos() {
    FILE *arq_txt = fopen(ARQUIVO_TXT, "r");
    if(arq_txt == NULL){
        printf("Erro ao abrir arquivo csv\n");
        exit(1);
    }
    FILE *arq_joias = fopen(ARQUIVO_JOIAS, "wb");
    FILE *arq_compras = fopen(ARQUIVO_COMPRAS, "wb");

    if(arq_joias == NULL || arq_compras == NULL){
        printf("Erro ao abrir arquivo binário\n");
        return;
    }

    char linha[300];
    char *campos[13];

    int capacidade_joias = 50000;
    int capacidade_compras = 100000;

    registro_joia *joias = malloc(capacidade_joias * sizeof(registro_joia));
    registro_compra *compras = malloc(capacidade_compras * sizeof(registro_compra));

    int cont_joias = 0;
    int cont_compras = 0;

    char *endptr;

    while(fgets(linha, sizeof(linha), arq_txt)){
        linha[strcspn(linha, "\r\n")] = '\0';

        if (linha[0] == '\0') {
            continue; //linha vazia
        }

        int n = splitar_linha(linha, campos, 13);
        if(n < 13) continue; //linha inválida

        int compra_existente = -1;
        int i;
        for (i = cont_compras - 1; i >= 0; i--) {
            uint64_t id_pedido_csv = strtoull(campos[1], NULL, 10);
            uint64_t id_produto_csv = strtoull(campos[2], NULL, 10);

            if (compras[i].id != id_pedido_csv) {
                break;
            }

            if (compras[i].id_produto == id_produto_csv) {
                compra_existente = i;
                break;
            }
        }

        if(compra_existente != -1){
            compras[compra_existente].quantidade += (strlen(campos[3]) > 0) ? atoi(campos[3]) : 1;
            printf("Duplicata encontrada: Pedido %s, Produto %s - Nova quantidade: %d\n",
            campos[1], campos[2], compras[compra_existente].quantidade);
        } else {
            registro_compra compra;

            preencher_campo_vazio(compra.data_hora, campos[0], sizeof(compra.data_hora));
            compra.id = strtoull(campos[1], &endptr, 10);
            compra.id_produto = strtoull(campos[2], &endptr, 10);
            compra.id_usuario = strtoull(campos[8], &endptr, 10);

            compra.quantidade = (strlen(campos[3]) > 0) ? atoi(campos[3]) : 1;
            compras[cont_compras++] = compra;
        }


        if (!produto_existe(joias, cont_joias, campos[2])) {
                registro_joia joia;
                joia.id = strtoull(campos[2], &endptr, 10);


                if (strlen(campos[6]) > 0) {
                    joia.id_marca = atoi(campos[6]);
                } else {
                    joia.id_marca = -1;
                }

                if (strlen(campos[7]) > 0) {
                    joia.preco = atof(campos[7]);
                } else {
                    joia.preco = 0.0;
                }

                if (strlen(campos[9]) > 0) {
                    joia.genero = campos[9][0];
                } else {
                    joia.genero = ' ';
                }

                preencher_campo_vazio(joia.cor_principal, campos[10], sizeof(joia.cor_principal));
                preencher_campo_vazio(joia.metal_principal, campos[11], sizeof(joia.metal_principal));
                preencher_campo_vazio(joia.joia_principal, campos[12], sizeof(joia.joia_principal));
                joia.excluido = 0;

                joias[cont_joias++] = joia;
            }
        }

    fclose(arq_txt);

    printf("Ordenando %d registros de joias...\n", cont_joias);
    qsort(joias, cont_joias, sizeof(registro_joia), comparar_joias);

    header_joias header;
    header.num_registros = cont_joias;
    header.num_excluidos = 0;
    header.num_insercoes = 0;

    fwrite(&header, sizeof(header_joias), 1, arq_joias);
    fwrite(joias, sizeof(registro_joia), cont_joias, arq_joias);
    fwrite(compras, sizeof(registro_compra), cont_compras, arq_compras);

    fclose(arq_joias);
    fclose(arq_compras);
    free(joias);
    free(compras);
}

////////////////////////////////////////////////
//////UTILITÁRIOS
//////////////////////////////////////////////

void mostrar_produto(registro_joia produto) {
    printf("\n=== PRODUTO ENCONTRADO ===\n");
    printf("ID: %llu\n", produto.id);
    printf("Marca: %d\n", produto.id_marca);
    printf("Preço: $%.2f\n", produto.preco);
    printf("Gênero: %c\n", produto.genero);
    printf("Cor: %s\n", produto.cor_principal);
    printf("Metal: %s\n", produto.metal_principal);
    printf("Joia: %s\n", produto.joia_principal);
}

void mostrar_compra(registro_compra compra) {
    printf("\n=== REGISTRO DE COMPRA ENCONTRADO ===\n");
    printf("ID Pedido: %llu\n", compra.id);
    printf("ID Produto: %llu\n", compra.id_produto);
    printf("Data/Hora: %s\n", compra.data_hora);
    printf("Quantidade: %d\n", compra.quantidade);
    printf("ID Usuário: %llu\n", compra.id_usuario);
}

//implementação bastante simplificada de um algoritmo para gerar ids snowflake, números de 64 bits baseados em timestamp
//utilizados como identificadores únicos em aplicações como twitter e discord
uint64_t gerar_id_joia() {
    const uint64_t EPOCH = 1130976092045ULL; // 2005-11-03 00:01:32 UTC
    static uint64_t ultimo_id = 0;

    struct timeval tv;
    gettimeofday(&tv, NULL);

    uint64_t now_ms = (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000ULL;
    uint64_t timestamp = now_ms - EPOCH;

    uint64_t id = (timestamp << 22);

    if (id <= ultimo_id) {
        id = ultimo_id + 1;
    }

    ultimo_id = id;
    return id;
}
///////////////////////////////////////////////////////////
///EXIBIÇÃO
/////////////////////////////////////////////////////////

void mostrar_joias(const char *filename) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        printf("Erro ao abrir arquivo %s\n", filename);
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, file);

    printf("%-19s %-6s %-10s %-6s %-12s %-12s %-12s\n",
           "ID", "Marca", "Preco", "Genero", "Cor", "Metal", "Tipo");
    printf("--------------------------------------------------------------------------------\n");

    registro_joia joia;
    int i;

    for(i = 0; i < header.num_registros; i++){
        fread(&joia, sizeof(registro_joia), 1, file);
        if(joia.excluido) continue;
        printf("%-19llu %-6d %-10.2f %-6c %-12s %-12s %-12s\n",
               joia.id,
               joia.id_marca,
               joia.preco,
               joia.genero,
               joia.cor_principal,
               joia.metal_principal,
               joia.joia_principal);
    }
    fclose(file);
    printf("\n");
}

void mostrar_compras(const char *filename) {
    FILE *arquivo = fopen(filename, "rb");
    if (!arquivo) {
        printf("Erro ao abrir arquivo %s\n", filename);
        return;
    }

    registro_compra compra;
    uint64_t idAnterior = -1;
    int primeiraLinha = 1;

    int cont = 0;

    //limitado a 1000 registros para não poluir a tela
    while (fread(&compra, sizeof(registro_compra), 1, arquivo) && cont < 1000) {

        if (compra.id != idAnterior) {
            if (!primeiraLinha) {
                printf("\n--------------------------------------------------\n");
            }
            printf("ID Pedido: %llu\n", compra.id);
            printf("Data/Hora: %s\n", compra.data_hora);
            printf("ID Usuário: %llu\n", compra.id_usuario);
            printf("Produtos:\n");

            idAnterior = compra.id;
            primeiraLinha = 0;
        }

        printf("  - %llu (Quantidade: %d)\n", compra.id_produto, compra.quantidade);
        cont++;
    }

    printf("--------------------------------------------------\n");
    fclose(arquivo);
}

void mostrar_dados(const char *tipo_arquivo) {
    if (strcmp(tipo_arquivo, "joias") == 0) {
        mostrar_joias(ARQUIVO_JOIAS);
    } else if (strcmp(tipo_arquivo, "compras") == 0) {
        mostrar_compras(ARQUIVO_COMPRAS);
    } else {
        printf("Tipo de arquivo inválido. Use: 'joias' ou 'compras'\n");
    }
}

///////////////////////////////////////////////////////////
///CONSULTAS
//////////////////////////////////////////////////////////

void consulta_contar_por_metal(char metal_procurado[]) {
    FILE *arq_joias = fopen(ARQUIVO_JOIAS, "rb");
    if (!arq_joias) {
        printf("Erro ao abrir arquivo de produtos\n");
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_joias);

    int contador = 0, i;
    registro_joia produto;

    for (i = 0; i < header.num_registros; i++) {
        fread(&produto, sizeof(registro_joia), 1, arq_joias);

        if ((strcasecmp(produto.metal_principal, metal_procurado) == 0) && !produto.excluido) {
            contador++;
        }
    }

    fclose(arq_joias);
    printf("Total de produtos: %d\n", contador);
}

void consulta_pedidos_por_usuario(uint64_t usuario_procurado) {
    FILE *arq_compras = fopen(ARQUIVO_COMPRAS, "rb");
    if (!arq_compras) {
        printf("Erro ao abrir arquivo de compras\n");
        return;
    }

    printf("\n=== PEDIDOS DO USUÁRIO %llu ===\n", usuario_procurado);
    registro_compra compra;
    int encontrados = 0;
    uint64_t pedido_atual = -1;
    int primeiro_pedido = 1;

    while (fread(&compra, sizeof(registro_compra), 1, arq_compras)) {
        if (compra.id_usuario == usuario_procurado) {
            if (pedido_atual != compra.id) {
                if (!primeiro_pedido) {
                    printf("\n");
                }
                printf(">>> PEDIDO: %llu | Data: %s\n", compra.id, compra.data_hora);
                printf("   Produto ID       | Qtd\n");
                printf("   -----------------+-----\n");
                pedido_atual = compra.id;
                primeiro_pedido = 0;
            }

            printf("   %llu | %-3d\n", compra.id_produto, compra.quantidade);
            encontrados++;
        }
    }

    fclose(arq_compras);

    if (encontrados == 0) {
        printf("Nenhum pedido encontrado para este usuário.\n");
    } else {
        printf("\nTotal de itens pedidos: %d\n", encontrados);
    }
}

void pesquisa_binaria_joia(uint64_t id_produto) {
    FILE *arq_joias = fopen(ARQUIVO_JOIAS, "rb");
    if (!arq_joias) {
        printf("Erro ao abrir arquivo de joias\n");
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_joias);

    int num_produtos = header.num_registros;

    int inicio = 0;
    int fim = num_produtos - 1;
    int encontrado = 0;
    registro_joia produto;

    while (inicio <= fim && !encontrado) {
        int meio = (inicio + fim) / 2;

        fseek(arq_joias, sizeof(header_joias) + meio * sizeof(registro_joia), SEEK_SET);
        fread(&produto, sizeof(registro_joia), 1, arq_joias);

        if (id_produto == produto.id) {
            encontrado = 1;
        } else if (id_produto < produto.id) {
            fim = meio - 1;
        } else {
            inicio = meio + 1;
        }
    }

    if (encontrado && !produto.excluido) {
        mostrar_produto(produto);
    } else {
        printf("Produto com ID '%llu' não encontrado.\n", id_produto);
    }

    fclose(arq_joias);
}

void pesquisa_binaria_compra(uint64_t id_pedido) {
    FILE *arq_compras = fopen(ARQUIVO_COMPRAS, "rb");
    if (!arq_compras) {
        printf("Erro ao abrir arquivo de compras\n");
        return;
    }

    fseek(arq_compras, 0, SEEK_END);
    long tamanho_arquivo = ftell(arq_compras);
    int num_compras = tamanho_arquivo / sizeof(registro_compra);
    rewind(arq_compras);

    int inicio = 0;
    int fim = num_compras - 1;
    int posicao_encontrada = -1;
    registro_compra compra;

    while (inicio <= fim) {
        int meio = (inicio + fim) / 2;

        fseek(arq_compras, meio * sizeof(registro_compra), SEEK_SET);
        fread(&compra, sizeof(registro_compra), 1, arq_compras);

        if (id_pedido == compra.id) {
            //continua para encontrar primeira ocorrência da compra
            posicao_encontrada = meio;
            fim = meio - 1;
        } else if (id_pedido < compra.id) {
            fim = meio - 1;
        } else {
            inicio = meio + 1;
        }
    }

    if (posicao_encontrada != -1) {
        printf("\n=== PEDIDO ENCONTRADO ===\n");

        fseek(arq_compras, posicao_encontrada * sizeof(registro_compra), SEEK_SET);
        fread(&compra, sizeof(registro_compra), 1, arq_compras);

        printf("ID Pedido: %llu\n", compra.id);
        printf("Data/Hora: %s\n", compra.data_hora);
        printf("ID Usuário: %llu\n", compra.id_usuario);

        printf("\nProdutos deste pedido:\n");
        printf("ID Produto         | Quantidade\n");
        printf("-------------------+-----------\n");

        int count = 0;
        int pos = posicao_encontrada;

        while (pos < num_compras) {
            fseek(arq_compras, pos * sizeof(registro_compra), SEEK_SET);
            fread(&compra, sizeof(registro_compra), 1, arq_compras);

            if (compra.id != id_pedido) {
                break;
            }

            printf("%-18llu | %-10d\n", compra.id_produto, compra.quantidade);
            count++;
            pos++;
        }

        printf("Total de itens no pedido: %d\n", count);
    } else {
        printf("Pedido com ID '%llu' não encontrado.\n", id_pedido);
    }

    fclose(arq_compras);
}

/////////////////////////////////////////////////////
////////// OPERAÇÕES
////////////////////////////////////////////////////

void reorganizar_arquivo_joias(indice_joias *indice) {
    FILE *arq_original = fopen(ARQUIVO_JOIAS, "rb");
    FILE *arq_temp = fopen("temp_joias.bin", "wb");

    if (!arq_original || !arq_temp) {
        printf("Erro ao reorganizar arquivo\n");
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_original);

    header_joias novo_header = {
        .num_registros = header.num_registros - header.num_excluidos,
        .num_excluidos = 0,
        .num_insercoes = 0
    };
    fwrite(&novo_header, sizeof(header_joias), 1, arq_temp);

    registro_joia joia;
    int copiados = 0;

    while (fread(&joia, sizeof(registro_joia), 1, arq_original)) {
        if (!joia.excluido) {
            fwrite(&joia, sizeof(registro_joia), 1, arq_temp);
            copiados++;
        }
    }

    fclose(arq_original);
    fclose(arq_temp);

    remove(ARQUIVO_JOIAS);
    rename("temp_joias.bin", ARQUIVO_JOIAS);

    liberar_indice_joias(indice);
    criar_indice_joias(indice);

    printf("Arquivo reorganizado: %d registros mantidos\n", copiados);
}

resultado_pesquisa pesquisa_joia_com_indice(indice_joias *indice, uint64_t id_produto) {
    resultado_pesquisa resultado = {0, -1, {0}};

    FILE *arq_produtos = fopen(ARQUIVO_JOIAS, "rb");
    if (!arq_produtos) {
        printf("Erro ao abrir arquivo de produtos\n");
        return resultado;
    }

    fseek(arq_produtos, sizeof(header_joias), SEEK_SET);

    int esquerda = 0, direita = indice->num_entradas - 1;
    int indice_intervalo = -1;
    while (esquerda <= direita) {
        int meio = (esquerda + direita) / 2;
        uint64_t chave_meio = indice->entradas[meio].chave;

        if (id_produto == chave_meio) {
            indice_intervalo = meio;
            break;
        } else if (id_produto < chave_meio) {
            direita = meio - 1;
        } else {
            esquerda = meio + 1;
        }
    }

    if (indice_intervalo == -1) {
        indice_intervalo = (direita >= 0) ? direita : 0;
    }

    long posicao_inicio = indice->entradas[indice_intervalo].posicao;
    long posicao_fim;

    if (indice_intervalo < indice->num_entradas - 1) {
        posicao_fim = indice->entradas[indice_intervalo + 1].posicao;
    } else {
        fseek(arq_produtos, 0, SEEK_END);
        posicao_fim = ftell(arq_produtos);
    }

    fseek(arq_produtos, posicao_inicio, SEEK_SET);

    printf("Buscando no intervalo %d (posições %ld a %ld)\n",
           indice_intervalo, posicao_inicio, posicao_fim);

    while (ftell(arq_produtos) < posicao_fim && !resultado.encontrado) {
        long posicao_atual = ftell(arq_produtos);

        if (!fread(&resultado.joia, sizeof(registro_joia), 1, arq_produtos)) {
            break;
        }

        if (id_produto == resultado.joia.id) {
            resultado.encontrado = 1;
            resultado.posicao = posicao_atual;
        } else if (id_produto < resultado.joia.id) {
            break;
        }
    }

    fclose(arq_produtos);
    return resultado;
}

int comparar_chaves(uint64_t id_pedido1, uint64_t id_produto1,
                    uint64_t id_pedido2, uint64_t id_produto2) {
    if (id_pedido1 < id_pedido2)
        return -1;
    if (id_pedido1 > id_pedido2)
        return 1;

    if (id_produto1 < id_produto2)
        return -1;
    if (id_produto1 > id_produto2)
        return 1;

    return 0;
}

void pesquisa_compra_com_indice(indice_compras *indice, uint64_t id_pedido, uint64_t id_produto) {
    FILE *arq_compras = fopen(ARQUIVO_COMPRAS, "rb");
    if (!arq_compras) {
        printf("Erro ao abrir arquivo de compras\n");
        return;
    }

    int esquerda = 0, direita = indice->num_entradas - 1;
    int indice_intervalo = -1;

    while (esquerda <= direita) {
        int meio = (esquerda + direita) / 2;
        uint64_t chave_pedido = indice->entradas[meio].id_pedido;
        uint64_t chave_produto = indice->entradas[meio].id_produto;

        int cmp_total = comparar_chaves(id_pedido, id_produto,
                                chave_pedido, chave_produto);

        if (cmp_total == 0) {
            indice_intervalo = meio;
            break;
        } else if (cmp_total < 0) {
            direita = meio - 1;
        } else {
            esquerda = meio + 1;
        }
    }

    if (indice_intervalo == -1)
        indice_intervalo = (direita >= 0) ? direita : 0;

    long posicao_inicio = indice->entradas[indice_intervalo].posicao;
    long posicao_fim;

    if (indice_intervalo < indice->num_entradas - 1)
        posicao_fim = indice->entradas[indice_intervalo + 1].posicao;
    else {
        fseek(arq_compras, 0, SEEK_END);
        posicao_fim = ftell(arq_compras);
    }

    int encontrado = 0;
    registro_compra compra;

    fseek(arq_compras, posicao_inicio, SEEK_SET);

    printf("Buscando no intervalo %d (posições %ld a %ld)\n",
           indice_intervalo, posicao_inicio, posicao_fim);

    while (ftell(arq_compras) < posicao_fim && !encontrado) {
        if (!fread(&compra, sizeof(registro_compra), 1, arq_compras))
            break;

        int cmp_total = comparar_chaves(id_pedido, id_produto,
                                compra.id, compra.id_produto);

        if (cmp_total == 0) {
            encontrado = 1;
        } else if (cmp_total < 0) {
            break;
        }
    }

    if (encontrado)
        mostrar_compra(compra);
    else
        printf("Compra com Pedido '%llu' e Produto '%llu' não encontrada.\n",
               id_pedido, id_produto);

    fclose(arq_compras);
}

void excluir_joia(indice_joias *indice, uint64_t id_produto) {
    resultado_pesquisa resultado = pesquisa_joia_com_indice(indice, id_produto);

    if (!resultado.encontrado || (resultado.encontrado && resultado.joia.excluido)) {
        printf("Produto com ID '%llu' não encontrado.\n", id_produto);
        return;
    }

    FILE *arq_produtos = fopen(ARQUIVO_JOIAS, "r+b");
    if (!arq_produtos) {
        printf("Erro ao abrir arquivo para exclusão\n");
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_produtos);
    header.num_excluidos++;
    fseek(arq_produtos, 0, SEEK_SET);
    fwrite(&header, sizeof(header_joias), 1, arq_produtos);

    fseek(arq_produtos, resultado.posicao, SEEK_SET);
    resultado.joia.excluido = 1;
    fwrite(&resultado.joia, sizeof(registro_joia), 1, arq_produtos);

    fclose(arq_produtos);

    printf("Produto '%llu' excluído logicamente.\n", id_produto);
    printf("Registros ativos: %d, Excluídos: %d\n", (header.num_registros - header.num_excluidos), header.num_excluidos);

    float percentual_excluidos = (float)header.num_excluidos / header.num_registros * 100;
    if (percentual_excluidos > 20.0) {
        printf("Mais de 20%% excluídos. Reorganizando arquivo...\n");
        reorganizar_arquivo_joias(indice);
    }
}

int inserir_joia(indice_joias *indice){
    FILE *arq_joias = fopen(ARQUIVO_JOIAS, "r+b");
    if (!arq_joias) {
        printf("Erro ao abrir arquivo de joias\n");
        return 0;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_joias);

    registro_joia nova_joia;

    printf("=== INSERIR NOVA JOIA ===\n");
    nova_joia.id = gerar_id_joia();

    printf("ID da marca: ");
    scanf("%d", &nova_joia.id_marca);

    printf("Preço: ");
    scanf("%f", &nova_joia.preco);

    printf("Gênero (m/f): ");
    scanf(" %c", &nova_joia.genero);

    printf("Cor principal: ");
    scanf("%9s", nova_joia.cor_principal);

    printf("Metal principal: ");
    scanf("%9s", nova_joia.metal_principal);

    printf("Joia principal: ");
    scanf("%9s", nova_joia.joia_principal);

    nova_joia.excluido = 0;

    long posicao_insercao = sizeof(header_joias) +
                           (header.num_registros * sizeof(registro_joia));
    printf("%l", posicao_insercao);

    fseek(arq_joias, posicao_insercao, SEEK_SET);
    fwrite(&nova_joia, sizeof(registro_joia), 1, arq_joias);

    header.num_registros++;
    header.num_insercoes++;

    fseek(arq_joias, 0, SEEK_SET);
    fwrite(&header, sizeof(header_joias), 1, arq_joias);

    printf("Joia inserida com sucesso! ID: %llu\n", nova_joia.id);
    printf("Inserções desde última reorganização: %d/%d\n",
           header.num_insercoes, MAX_INSERCOES);

    if (header.num_insercoes >= MAX_INSERCOES) {
        liberar_indice_joias(indice);
        criar_indice_joias(indice);

        FILE *arq = fopen(ARQUIVO_JOIAS, "r+b");
        if (arq) {
            header_joias h;
            fread(&h, sizeof(header_joias), 1, arq);
            h.num_insercoes = 0;
            fseek(arq, 0, SEEK_SET);
            fwrite(&h, sizeof(header_joias), 1, arq);
            fclose(arq);
        }
    }

    fclose(arq_joias);
    return 1;
}
////////////////////////////////////////////////////
//////MENUS
///////////////////////////////////////////////////

void menu_mostrar_dados() {
    int sub_opcao;

    do {
        printf("\n=== MOSTRAR DADOS ===\n");
        printf("1. Mostrar produtos\n");
        printf("2. Mostrar compras\n");
        printf("0. Voltar\n");
        printf("Escolha: ");
        scanf("%d", &sub_opcao);

        switch(sub_opcao) {
            case 1:
                mostrar_dados("joias");
                break;

            case 2:
                mostrar_dados("compras");
                break;
            case 0:
                break;

            default:
                printf("Opção inválida!\n");
        }
    } while (sub_opcao != 0 && sub_opcao != 3);
}

void menu_consultas() {
    int opcao;
    char metal_procurado[10];
    uint64_t usuario_procurado;
    uint64_t id_produto;
    uint64_t id_pedido;

    do {
        printf("\n=== MENU DE CONSULTAS ===\n");
        printf("1. Contar produtos por metal\n");
        printf("2. Pedidos por usuário\n");
        printf("3. Consultar joia por ID (sem índice)\n");
        printf("4. Consultar compra por ID (sem índice)\n");
        printf("0. Voltar\n");
        printf("Escolha: ");
        scanf("%d", &opcao);

        switch(opcao) {
            case 1:
                printf("Digite o metal para consulta (ex: gold, silver, platinum): ");
                scanf("%s", metal_procurado);
                consulta_contar_por_metal(metal_procurado);
                break;

            case 2:
                printf("Digite o ID do usuário para consulta: ");
                scanf("%llu", &usuario_procurado);
                consulta_pedidos_por_usuario(usuario_procurado);
                break;

            case 3:
                printf("Digite o ID do produto para buscar: ");
                scanf("%llu", &id_produto);
                pesquisa_binaria_joia(id_produto);
                break;

            case 4:
                printf("Digite o ID do pedido para buscar: ");
                scanf("%llu", &id_pedido);
                pesquisa_binaria_compra(id_pedido);
                break;

            case 0:
                printf("Voltando ao menu principal...\n");
                break;

            default:
                printf("Opção inválida!\n");
        }
    } while (opcao != 0);
}

void menu_operacoes(indice_joias *idx_joias, indice_compras *idx_compras) {
    int opcao;
    uint64_t id_produto;
    uint64_t id_pedido;

    do {
        printf("\n=== OPERAÇÕES COM OS DADOS ===\n");
        printf("1. Consultar joia (usando índice)\n");
        printf("2. Consultar único registro de compra (usando índice)\n");
        printf("3. Cadastrar joia\n");
        printf("4. Excluir joia\n");
        printf("0. Voltar\n");
        printf("Escolha: ");
        scanf("%d", &opcao);

        switch(opcao) {
            case 1:
                printf("Digite o ID do produto para buscar: ");
                scanf("%llu", &id_produto);
                {
                    resultado_pesquisa resultado = pesquisa_joia_com_indice(idx_joias, id_produto);
                    if (resultado.encontrado && !resultado.joia.excluido) {
                        mostrar_produto(resultado.joia);
                    } else {
                        printf("Produto com ID '%llu' não encontrado.\n", id_produto);
                    }
                }
                break;

            case 2:
                printf("Digite o ID do pedido para buscar: ");
                scanf("%llu", &id_pedido);
                printf("Digite o ID do produto para buscar: ");
                scanf("%llu", &id_produto);
                pesquisa_compra_com_indice(idx_compras, id_pedido, id_produto);
                break;
            case 3:
                int resultado = inserir_joia(idx_joias);
                if(resultado){
                    printf("Joia inserida com sucesso\n");
                }
                break;

            case 4:
                printf("Digite o ID do produto para excluir: ");
                scanf("%llu", &id_produto);
                excluir_joia(idx_joias, id_produto);
                break;

            case 0:
                printf("Voltando ao menu principal...\n");
                break;

            default:
                printf("Opção inválida!\n");
        }
    } while (opcao != 0);
}

void menu_principal(indice_joias *idx_joias, indice_compras *idx_compras) {
    int opcao;

    do {
        printf("\n=== MENU PRINCIPAL ===\n");
        printf("1. Mostrar dados dos arquivos\n");
        printf("2. Consultas\n");
        printf("3. Operações\n");
        printf("0. Sair\n");
        printf("Escolha: ");
        scanf("%d", &opcao);

        switch(opcao) {
            case 1:
                menu_mostrar_dados();
                break;
            case 2:
                menu_consultas();
                break;
            case 3:
                menu_operacoes(idx_joias, idx_compras);
                break;
            case 0:
                printf("Saindo...\n");
                break;
            default:
                printf("Opção inválida!\n");
        }
    } while (opcao != 0);
}

//para testar a reconstrução do índice
void inserir_varias_joias() {
    FILE *arq_joias = fopen(ARQUIVO_JOIAS, "r+b");
    if (!arq_joias) {
        printf("Erro ao abrir arquivo de joias\n");
        return;
    }

    header_joias header;
    fread(&header, sizeof(header_joias), 1, arq_joias);

    int i;
    for (i = 0; i < MAX_INSERCOES - 1; i++) {
        registro_joia nova_joia;
        nova_joia.id = gerar_id_joia();
        nova_joia.id_marca = 1;
        nova_joia.preco = 100.00;
        nova_joia.genero = 'f';
        strcpy(nova_joia.cor_principal, "red");
        strcpy(nova_joia.metal_principal, "gold");
        strcpy(nova_joia.joia_principal, "topaz");
        nova_joia.excluido = 0;

        long posicao_insercao = sizeof(header_joias) +
                               (header.num_registros * sizeof(registro_joia));

        fseek(arq_joias, posicao_insercao, SEEK_SET);
        fwrite(&nova_joia, sizeof(registro_joia), 1, arq_joias);

        header.num_registros++;
        header.num_insercoes++;

        printf("Inserida joia %d — ID: %llu\n", i + 1, nova_joia.id);
    }

    fseek(arq_joias, 0, SEEK_SET);
    fwrite(&header, sizeof(header_joias), 1, arq_joias);

    fclose(arq_joias);
    printf("\nInserção automática concluída. Total de registros: %d\n", header.num_registros);
}

int main() {
    FILE *dados = fopen(ARQUIVO_COMPRAS, "rb");
    if(dados == NULL){
        criar_arquivos();
    }

    indice_joias idx_joias;
    indice_compras idx_compras;
    int resultado_j = carregar_indice_joias(&idx_joias);
    int resultado_c = carregar_indice_compras(&idx_compras);

    if(!resultado_j){
        criar_indice_joias(&idx_joias);
    }

    if(!resultado_c) {
        criar_indice_compras(&idx_compras);
    }

    //inserir_varias_joias();

    menu_principal(&idx_joias, &idx_compras);
    salvar_indices(&idx_joias, &idx_compras);
    liberar_indice_compras(&idx_compras);
    liberar_indice_joias(&idx_joias);
    return 0;
}
