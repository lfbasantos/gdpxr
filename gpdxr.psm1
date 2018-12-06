#
# 2018 - gpdxr - Greenplum DDL Extractor & Replicator
# PowerShell Tool for Greenplum Schema Replication
# by luiz.filipe@remay.com.br / lfbasantos @gmail.com
# Script Modulo de Funcoes

# 
#FUNCAO MENSAGEM (CENTRALIZA O ENVIO DE MENSAGENS DO SCRIPT)
    function sendMsg {
    Param([int] $a, [int] $b, [string] $c)
        $Stamp = Get-Date -f o
        switch ($a) { 
            1 {
                switch ($b) {
                #
                # INICIALIZACAO E FINALIZACAO
                100 {$vMsg = "[GDEXREP][${Stamp}]: gpdxr - Greenplum DDL Extractor & Replicator v.1.0.3"}
                101 {$vMsg = "[GDEXREP][${Stamp}]: Iniciando processamento"}
                102 {$vMsg = "[GDEXREP][${Stamp}]: Finalizado com erro"}
                103 {$vMsg = "[GDEXREP][${Stamp}]: Finalizado com Sucesso"}
                #
                # MENSAGENS GERAIS E DE ERRO
                1000 {$vMsg = "[GDEXREP][${Stamp}]: Parametro 'pghome' nao enviado"}
                1001 {$vMsg = "[GDEXREP][${Stamp}]: Formato de parametro invalido"}
                1002 {$vMsg = "[GDEXREP][${Stamp}]: Parametro deve iniciar com '--'"}
                1003 {$vMsg = "[GDEXREP][${Stamp}]: Valor nao encontrado (sinal de igual) 'sintaxe: --param=<valor>'"}
                1004 {$vMsg = "[GDEXREP][${Stamp}]: Nao pode haver dois prefixos '--' no mesmo parametro"}
                1005 {$vMsg = "[GDEXREP][${Stamp}]: Digite GDEXREP --help para ajuda"}
                1006 {$vMsg = "[GDEXREP][${Stamp}]: Parametro 'sametarget' nao definido"}
                1007 {$vMsg = "[GDEXREP][${Stamp}]: Parametro sametarget = off mas Host 2 nao esta configurado"}
                1008 {$vMsg = "[GDEXREP][${Stamp}]: Parametro 'workarea' nao encontrado"}
                1009 {$vMsg = "[GDEXREP][${Stamp}]: Parametro 'ddlmode' nao encontrado"}
                1010 {$vMsg = "[GDEXREP][${Stamp}]: Parametros de URL nao encontrado"}
                1011 {$vMsg = "[GDEXREP][${Stamp}]: Parametro '--mode' nao encontrado"}
				1012 {$vMsg = "[GDEXREP][${Stamp}]: Contagem de registros nao bate"}
				1013 {$vMsg = "[GDEXREP][${Stamp}]: Verifique a diferenca entre a tabela original e a nova"}
				1014 {$vMsg = "[GDEXREP][${Stamp}]: "}
				1015 {$vMsg = "[GDEXREP][${Stamp}]: "}
				1016 {$vMsg = "[GDEXREP][${Stamp}]: "}
                #
                # MENSAGENS DE AJUDA
                2000 {$vMsg = "[GDEXREP][${Stamp}]: Iniciando Ajuda"}
                2001 {$vMsg = "GDEXREP Help System"}
                2002 {$vMsg = "Sintaxe: gpdxr --params "}
                2003 {$vMsg = "Where params must be inside double quotes :"}
                2004 {$vMsg = "--mode=<1,2>             - 1 for extract only, and 2 for extract from source and apply on target"}
                2005 {$vMsg = "--sametarget==off        - to be used when destination host for schema replication is different from source host."}
                2006 {$vMsg = "--url=JSON Format        - source and target hosts. Host2 is only to be set when --sametarget param is off. Json like Syntax: {'urls':[{url}],[{url}]} where 'url' is: 'url':'<#>', 'user':'<user>', 'pass':'<pass>', 'host':'<host>', 'port':'<port>', 'database':'<database>', 'schema':'<schema>'"}
                2007 {$vMsg = "--pghome=<dir>           - directory of Postgres / Greenplum home for psql and pg-dump location"}
                #
                # Mensagens de Depuracao
                3000 {$vMsg = "[GDEXREP][${Stamp}]: Configurando variaveis de conexao com Banco de Dados"}
                3001 {$vMsg = "[GDEXREP][${Stamp}]: Variaveis configuradas"}
                3002 {$vMsg = "[GDEXREP][${Stamp}]: Modo 1 (Extract) Selecionado"}
                3003 {$vMsg = "[GDEXREP][${Stamp}]: Arquivo DDL Gerado"}
                3004 {$vMsg = "[GDEXREP][${Stamp}]: Modo 2 (Replace) selecionado"}
                3005 {$vMsg = "[GDEXREP][${Stamp}]: Replace de Compressao iniciado"}
                3006 {$vMsg = "[GDEXREP][${Stamp}]: Verificando clausula WITH"}
                3007 {$vMsg = "[GDEXREP][${Stamp}]: Não existe WITH, criando string para inserir no arquivo de DDL"}
                3008 {$vMsg = "[GDEXREP][${Stamp}]: WITH e Compression existem, substituindo compress level"}
                3009 {$vMsg = "[GDEXREP][${Stamp}]: Realizando dump da DDL"}
                3010 {$vMsg = "[GDEXREP][${Stamp}]: DDL gerado com sucesso"}
                3011 {$vMsg = "[GDEXREP][${Stamp}]: Renomeando tabela"}
                3012 {$vMsg = "[GDEXREP][${Stamp}]: Novo nome aplicado ao arquivo de schema auxiliar"}
                3013 {$vMsg = "[GDEXREP][${Stamp}]: 3013 - Erro no processamento." }
                3014 {$vMsg = "[GDEXREP][${Stamp}]: Atribuindo schema de trabalho na DDL"}
                3015 {$vMsg = "[GDEXREP][${Stamp}]: DDL atualizada"}
                3016 {$vMsg = "[GDEXREP][${Stamp}]: Criando tabela replica"}
                3017 {$vMsg = "[GDEXREP][${Stamp}]: Tabela replica criada"}
                3018 {$vMsg = "[GDEXREP][${Stamp}]: Erro Aplicando estrutura, verifique se tabela _new ja existe."}
                3019 {$vMsg = "[GDEXREP][${Stamp}]: Carga de dados na tabela replica"}
                3020 {$vMsg = "[GDEXREP][${Stamp}]: Erro ao realizar insert de dados."}
                3021 {$vMsg = "[GDEXREP][${Stamp}]: insert na tabela replica realizado com sucesso"}
                3022 {$vMsg = "[GDEXREP][${Stamp}]: verificando count de registros"}
                3023 {$vMsg = "[GDEXREP][${Stamp}]: Erro contando registros"}
                3024 {$vMsg = "[GDEXREP][${Stamp}]: Count de registros realizado com sucesso"}
                3025 {$vMsg = "[GDEXREP][${Stamp}]: Contagem de registros confere"}
                3026 {$vMsg = "[GDEXREP][${Stamp}]: Renomear tabela original para BKP"}
				3027 {$vMsg = "[GDEXREP][${Stamp}]: Erro ao renomear"}
				3028 {$vMsg = "[GDEXREP][${Stamp}]: Rename da tabela original para bkp realizado com sucesso"}
				3029 {$vMsg = "[GDEXREP][${Stamp}]: Alter Schema de tabela Bkp para WorkArea"}
				3030 {$vMsg = "[GDEXREP][${Stamp}]: Falha ao realizar Alter Schema"}
				3031 {$vMsg = "[GDEXREP][${Stamp}]: Alter Schema com sucesso"}
				3032 {$vMsg = "[GDEXREP][${Stamp}]: Renomear tabela nova para tabela original"}
				3033 {$vMsg = "[GDEXREP][${Stamp}]: Erro ao realizar rename"}
				3034 {$vMsg = "[GDEXREP][${Stamp}]: Tabela nova renomeada com sucesso"}
				3035 {$vMsg = "[GDEXREP][${Stamp}]: transfere tabela new para schema origem"}
				3036 {$vMsg = "[GDEXREP][${Stamp}]: Falha: alter table "}
				3037 {$vMsg = "[GDEXREP][${Stamp}]: alter table com sucesso"}
				30 {$vMsg = "[GDEXREP][${Stamp}]: "}
                }
            }
            2 {
                switch ($b) {
                #
                # 
                1000 {$vMsg = "'pghome' parameter is not set."}
                }
            }
        }
    
     #
     # MOSTRA MENSAGEM GERAL
        if ($c) {
            $vMsg = "[GDEXREP][${Stamp}]: ${c}"
            Write-Host $vMsg
        } else {
            Write-Host $vMsg
        }
    }

#
# FUNCAO DEBUG
    function depuradorLigado {
        sendMsg -c "PGHOME   :$global:vPGHome"
        sendMsg -c "MODE     :$global:vMode"
        sendMsg -c "Tabela   :$global:vTabela"
        sendMsg -c "Schema   :$global:vSchema"
        sendMsg -c "Work     :$global:vWrkSchema"
        sendMsg -c "LevelTo  :$global:vLevelTo"
        sendMsg -c "TypeTo   :$global:vTypeTo"
        sendMsg -c "DDLMode  :$global:vDDLMode"
        sendMsg -c "DB-User  :$global:vDBUser"
        sendMsg -c "DP-Pass  :$global:vDBPass"
        sendMsg -c "DB-Host  :$global:vDBHost"
        sendMsg -c "DB-Port  :$global:vDBPort"
        sendMsg -c "DB-Name  :$global:vDBName"
        sendMsg -c "DB-Schema:$global:vDBSchm"
    }

#
# FUNCAO AJUDA
    function ajudeMe {
        sendMsg -a 1 -b 2000
        sendMsg -a 1 -b 2001
        sendMsg -a 1 -b 2002
        sendMsg -a 1 -b 2003
        sendMsg -a 1 -b 2004
        sendMsg -a 1 -b 2005
        sendMsg -a 1 -b 2006
        sendMsg -a 1 -b 2007
    }

#
# MENSAGENS DE INICIALIZACAO DO SCRIPT
    function inicializaScript {
        sendMsg -a 1 -b 100
        sendMsg -a 1 -b 101
    }

#
# FINALIZA SCRIPT COM ERRO
    function finalizaErro {
        sendMsg -a 1 -b 102
    }

#
# FINALIZA SCRIPT COM SUCESSO
    function finalizaSucesso {
        sendMsg -a 1 -b 103
    }

#
# ELIMINA ARQUIVOS TEMPORARIOS
    function limpaTemporarios {
        Remove-Item -Path insert.sql      -ErrorAction SilentlyContinue
        Remove-Item -Path rename1.sql     -ErrorAction SilentlyContinue
        Remove-Item -Path rename2.sql     -ErrorAction SilentlyContinue
        Remove-Item -Path ddl.sql         -ErrorAction SilentlyContinue
        Remove-Item -Path ddl.new.sql     -ErrorAction SilentlyContinue
        Remove-Item -Path ddlaux1.sql     -ErrorAction SilentlyContinue
        Remove-Item -Path ddlaux2.sql     -ErrorAction SilentlyContinue
        Remove-Item -Path count1.sql      -ErrorAction SilentlyContinue
        Remove-Item -Path count2.sql      -ErrorAction SilentlyContinue
        Remove-Item -Path cmd.log         -ErrorAction SilentlyContinue
        Remove-Item -Path ddlaux.new.sql  -ErrorAction SilentlyContinue
        Remove-Item -Path ddlaux2.new.sql -ErrorAction SilentlyContinue
        Remove-Item -Path alter1.sql      -ErrorAction SilentlyContinue
        Remove-Item -Path alter2.new.sql  -ErrorAction SilentlyContinue
    }


#
# VALIDACAO DE SINTAXE PARAMETROS
    function sintaxeParametros {
        $pQtd = $args.Count
	    for ($i = 1; $i -le $pQtd; $i++) {
            $vStr = $args[$i - 1]
            if ($vStr.Substring(0, 2) -ne "--") {
                sendMsg -a 1 -b 1001
                sendMsg -a 1 -b 1002
                sendMsg -c ">>[${vStr}]"
                return $true
            }
            elseif($vStr.IndexOf("=") -eq -1) {
                sendMsg -a 1 -b 1001
                sendMsg -a 1 -b 1003
                sendMsg -c ">>[${vStr}]"
                return $true
            }
            elseif(([regex]::Matches($vStr, "--")).count -eq 2) {
                sendMsg -a 1 -b 1001
                sendMsg -a 1 -b 1004
                sendMsg -c ">>[${vStr}]"
                return $true
            }
        }
    }

#
# VARREDURA DE PARAMETROS
    function varreduraParametros($parametro, $qtd, [string[]] $pargs) {
	    for ($i = 1; $i -le $qtd; $i++) {
            $vStr = $pargs[$i - 1]
            if (([regex]::Matches($vStr, $parametro)).count -eq 1) {
                $vRetorno = $vStr.Substring($vStr.IndexOf("=") + 1, $vStr.length - $vStr.IndexOf("=") - 1)
                $vCheck = 1
            } 
        }

        if ($vCheck -eq 1) {
            return $vRetorno
        } else {
            return 0
        }
    }
	
#
# Verifica parametros Json - Compression
    function jsonParamCompressao($qtd, [string[]] $pargs) {
	    for ($i = 1; $i -le $qtd; $i++) {
            $vStr = $pargs[$i - 1]
            if (([regex]::Matches($vStr, "--compression")).count -eq 1) {
                $global:vCompression = $vStr.Substring($vStr.IndexOf("=") + 1, $vStr.length - $vStr.IndexOf("=") - 1)
                $vArr = ConvertFrom-Json -InputObject $global:vCompression
                $global:vLevelFrom = $vArr.compressions | Where-Object {    $_.compression -eq 'from'   } | Select-Object -expand level
                $global:vTypeFrom = $vArr.compressions | Where-Object {    $_.compression -eq 'from'   } | Select-Object -expand type
                $global:vLevelTo = $vArr.compressions | Where-Object {    $_.compression -eq 'to'   } | Select-Object -expand level
                $global:vTypeTo = $vArr.compressions | Where-Object {    $_.compression -eq 'to'   } | Select-Object -expand type
				$vCheck = 1
            } 
        }

    }

#
# Verifica parametros Json - URL
    function jsonParamURL($qtd, [string[]] $pargs) {
	    for ($i = 1; $i -le $qtd; $i++) {
            $vStr = $pargs[$i - 1]
            if (([regex]::Matches($vStr, "--url")).count -eq 1) {
                $vUrl = $vStr.Substring($vStr.IndexOf("=") + 1, $vStr.length - $vStr.IndexOf("=") - 1)
                $vArr = ConvertFrom-Json -InputObject $vUrl
                $global:vDBUser = $vArr.urls | Where-Object {    $_.url -eq 1   } | Select-Object -expand user
                $global:vDBPass = $vArr.urls | Where-Object {    $_.url -eq 1   } | Select-Object -expand pass
                $global:vDBHost = $vArr.urls | Where-Object {    $_.url -eq 1   } | Select-Object -expand host
                $global:vDBPort = $vArr.urls | Where-Object {    $_.url -eq 1   } | Select-Object -expand port
                $global:vDBName = $vArr.urls | Where-Object {    $_.url -eq 1   } | Select-Object -expand database
                $global:vDBSchm = $vArr.urls | Where-Object {    $_.url -eq 1   } | Select-Object -expand schema
				$vCheck = 1
            } 
        }

    }
	
	
