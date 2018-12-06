#
# 2018 - gpdxr - Greenplum DDL Extractor & Replicator
# PowerShell Tool for Greenplum Schema Replication
# by luiz.filipe@remay.com.br / lfbasantos@gmail.com
#
$OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues = @{'Out-File:Encoding' = 'utf8'}
$vCurrentDir = ".\"
$vDHRFile = Get-Date -f yyyyMMddHHmmss

#
# IMPORT FUNCTIONS
Unblock-File -Path "${vCurrentDir}gpdxr.psm1"
Import-Module "${vCurrentDir}gpdxr.psm1" -Force

#
# INICIO DO PROCESSAMENTO
inicializaScript
limpaTemporarios

#
# FINALIZA SE HOUVE CHAMADA PARA HELP
$vBreakHelp = varreduraParametros -parametro "--help"
if ($vBreakHelp) {
    ajudeMe
    Break
}

#
# FINALIZA SE HOUVER SINTAXE INCORRETA
$vBreakSintaxe = sintaxeParametros
if ($vBreakSintaxe) {
    sendMsg -a 1 -b 1005
    sendMsg -a 1 -b 102
    Break
}

#
# Parametros Gerais
$global:vDebug = varreduraParametros -parametro "--debug" -qtd $args.Count -pargs $args
$global:vMode = varreduraParametros -parametro "--mode" -qtd $args.Count -pargs $args
$global:vTabela = varreduraParametros -parametro "--table" -qtd $args.Count -pargs $args
$global:vSchema = varreduraParametros -parametro "--schema" -qtd $args.Count -pargs $args
if ((varreduraParametros -parametro "--ddlmode" -qtd $args.Count  -pargs $args) -eq "on") { $global:vDDLMode = "--schema-only" } else { $global:vDDLMode="" }
$global:vWrkSchema = varreduraParametros -parametro "--workarea" -qtd $args.Count  -pargs $args
$global:vPGHome = varreduraParametros -parametro "--pghome" -qtd $args.Count  -pargs $args


#
# Parametros de Compressao
jsonParamCompressao -qtd $args.Count -pargs $args

#
# Parametros de URL
jsonParamURL -qtd $args.Count -pargs $args

#
# MOSTRA VARIAVEIS E PARAMETROS NA TELA (DEPURADOR)
if ($vDebug -eq 1) {
    depuradorLigado
}

#
# VERIFICA PARAMETROS OBRIGATORIOS# OBRIGATORIO ENVIAR PARAMETRO PGHOME
if (!$vPGHome) {
    sendMsg -a 1 -b 1000
    $vBreak = $true
} else {
    #
    # CONFIGURACAO DOS COMANDOS PSQL E PGDUMP
    $cmdPsql = "${vPGHome}\psql.exe"
    $cmdPgdump = "${vPGHome}\pg_dump.exe"
}
   
if (!$vWrkSchema) {
    sendMsg -a 1 -b 1008
    $vBreak = $true
}

if (!$vDDLMode) {
    sendMsg -a 1 -b 1009
    $vBreak = $true
}

if (!$vDBHost) {
    sendMsg -a 1 -b 1010
    $vBreak = $true
}

if (!$vMode) {
    sendMsg -a 1 -b 1011
    $vBreak = $true
}

#
# FINALIZA SE QUALQUER UMA DAS VALIDACOES FALHAR
if ($vBreak) {
    sendMsg -a 1 -b 1005
    sendMsg -a 1 -b 102
    Break
}

#
# CONFIGURA VARIAVEIS DE CONEXAO
if ($vDebug -eq 1) { sendMsg -a 1 -b 3000 }
$env:PGDATABASE = $vDBName
$env:PGHOST = $vDBHost
$env:PGPASSWORD = $vDBPass
$env:PGUSER = $vDBUser
$env:PGPORT = $vDBPort
if ($vDebug -eq 1) { sendMsg -a 1 -b 3001 }


#
# MODO 1 - APENAS EXTRAI SCHEMA
if ($vMode -eq 1) {
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3002 }
    cmd /c $cmdPgdump --table=$vTabela --file=ddl.${vDHRFile}.sql $vDDLMode
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3003 }
} 
else {
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3004 }
    # Modo 2 - REPLACE
    # Dump Schema
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3009 }
    cmd /c $cmdPgdump --table=$vTabela --file=ddl.sql $vDDLMode

    # Verifica se foi solicitado troca de compressao
    if ($vCompression) {
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3005 }
        # Verifica se DDL gerado possui clausula WITH
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3006 }
        $vCheckWith = Select-String $vCurrentDir\ddl.sql -Pattern WITH -Quiet -SimpleMatch -CaseSensitive
        $vCheckComp = Select-String $vCurrentDir\ddl.sql -Pattern compresstype -Quiet -SimpleMatch

    #
    # Validacao clausula WITH e Compressao
        if (!$vCheckWith) {
            if ($vDebug -eq 1) { sendMsg -a 1 -b 3007 }
            $vPara = "WITH (APPENDONLY=TRUE, COMPRESSLEVEL=${vLevelTo}, COMPRESSTYPE=${vTypeTo}) DISTRIBUTED"
            (Get-Content ddl.sql).replace("DISTRIBUTED", $vPara) | Set-Content ddlaux2.sql
        } elseif($vCheckWith -and $vCheckComp) {
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3008 }
            $vDe = "compresslevel=${vLevelFrom}"
            $vPara = "compresslevel=${vLevelTo}"
            (Get-Content ddl.sql) -replace "compresslevel=([0-9]+)", "$vPara" | Set-Content ddlaux1.sql
            $vDe = "compresstype=${vTypeFrom}"
            $vPara = "compresstype=${vTypeTo}"
            (Get-Content ddlaux1.sql) -replace "compresstype=([a-z]+)", "$vPara" | Set-Content ddlaux2.sql
        }
    }
	if ($vDebug -eq 1) { sendMsg -a 1 -b 3010 }
	
    # 
    # Renomeando tabela
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3011 }
    $vNewTable = "${vTabela}_new"
    $vTabela = $vTabela.Substring($vTabela.IndexOf(".") + 1, $vTabela.length - $vTabela.IndexOf(".") - 1)
    $vNewTable = $vNewTable.Substring($vNewTable.IndexOf(".") + 1, $vNewTable.length - $vNewTable.IndexOf(".") - 1)
	try {
        (Get-Content ddlaux2.sql) -replace "$vTabela", "$vNewTable" | Set-Content ddlaux.new.sql
	} catch {
        if ($vDebug -eq 1) {
		    sendMsg -a 1 -b 3013
            sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		} else {
            sendMsg -a 1 -b 3013
        }
	}
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3012 }
	
	#
	# Atribui schema de trabalho ao arquivo de DDL
	if ($vDebug -eq 1) { sendMsg -a 1 -b 3014 }
    $vNewSearchPath = "SET search_path = ${vWrkSchema}, "
    (Get-Content ddlaux.new.sql) -replace "SET search_path = ([a-z]+),", $vNewSearchPath | Set-Content ddlaux2.new.sql
    $vNewAlterTable = "ALTER TABLE ${vWrkSchema}."
    (Get-Content ddlaux2.new.sql) -replace "ALTER TABLE ([a-z]+).", $vNewAlterTable | Set-Content ddl.new.sql
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3015 }

    #
    # Aplica estrutura 
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3016 }
	try {
        $vTranscriptFile = "${vCurrentdir}cmd.log"
        Start-Transcript -Path $vTranscriptFile
        cmd /c $cmdPsql -f ddl.new.sql
        Stop-Transcript
        $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
        if ($vChkErr) {
            sendMsg -a 1 -b 3018
            finalizaErro
        Break
        }
    } catch {
        if ($vDebug -eq 1) {
		    sendMsg -a 1 -b 3013
            sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		} else {
            sendMsg -a 1 -b 3013
        }
	}
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3017 }
	
    #
    # Carga de dados na tabela replica
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3019 }
    try {
        "insert into ${vWrkSchema}.${vTabela}_new select * from ${vDBSchm}.${vTabela}" | Set-Content insert.sql
        $vTranscriptFile = "${vCurrentdir}cmd.log"
        Start-Transcript -Path $vTranscriptFile
        cmd /c $cmdPsql -f insert.sql
        Stop-Transcript
        $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
        if ($vChkErr) {
		    sendMsg -a 1 -b 3020
            finalizaErro
            Break
        }
    } catch {
        if ($vDebug -eq 1) {
		    sendMsg -a 1 -b 3013
            sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		} else {
            sendMsg -a 1 -b 3013
        }
	}
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3021 }
	
    #
    # Verifica count de registros (check de qualidade)
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3022 }
	try {
        "select count(*) qtd from ${vDBSchm}.${vTabela}" | Set-Content count1.sql
        $vTranscriptFile = "${vCurrentdir}cmd.log"
        Start-Transcript -Path $vTranscriptFile
        cmd /c $cmdPsql -f count1.sql
        $vResult1 = cmd /c $cmdPsql -f count1.sql
        Stop-Transcript
        $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
        if ($vChkErr) {
            if ($vDebug -eq 1) { sendMsg -a 1 -b 3023 }
			sendMsg -c "${vDBSchm}.${vTabela}"
            finalizaErro
            Break
        }
    } catch {
        if ($vDebug -eq 1) {
		    sendMsg -a 1 -b 3013
            sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		} else {
            sendMsg -a 1 -b 3013
        }
	}
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3024 }

    #
    # Verifica count de registros (check de qualidade)
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3022 }
	try {
        "select count(*) qtd from ${vWrkSchema}.${vTabela}_new" | Set-Content count2.sql
        $vTranscriptFile = "${vCurrentdir}cmd.log"
        Start-Transcript -Path $vTranscriptFile
        cmd /c $cmdPsql -f count2.sql
        $vResult2 = cmd /c $cmdPsql -f count2.sql
        Stop-Transcript
        $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
        if ($vChkErr) {
            if ($vDebug -eq 1) { sendMsg -a 1 -b 3023 }
			sendMsg -c "${vWrkSchema}.${vTabela}_new"
            finalizaErro
            Break
        }
    } catch {
        if ($vDebug -eq 1) {
		    sendMsg -a 1 -b 3013
            sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		} else {
            sendMsg -a 1 -b 3013
        }
	}
    if ($vDebug -eq 1) { sendMsg -a 1 -b 3024 }
	

    #
    # Check de Qualidade - Validando comparacao de contagem
    if ($vResult1[2] -ne $vResult2[2]) {
        sendMsg -a 1 -b 1012
		sendMsg -a 1 -b 1013
		Break
    } else {
	    #
		# Somente prossegue se a validacao de qualidade bater
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3025 }

        #
        # Renomear tabelas
		if ($vDebug -eq 1) { sendMsg -a 1 -b 3026 }
		try {
            $vTabelaAux = $vTabela.Substring($vTabela.IndexOf(".") + 1, $vTabela.length - $vTabela.IndexOf(".") - 1)
            "alter table ${vDBSchm}.${vTabela} rename to ${vTabelaAux}_bkp" | Set-Content rename1.sql
            $vTranscriptFile = "${vCurrentdir}cmd.log"
            Start-Transcript -Path $vTranscriptFile
            cmd /c $cmdPsql -f rename1.sql -t
            Stop-Transcript
            $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
            if ($vChkErr) {
			    if ($vDebug -eq 1) { sendMsg -a 1 -b 3027 }
                sendMsg -c "${vTabela}, ${vTabelaAux}_bkp]"
                finalizaErro
                Break
            }
		} catch {
            if ($vDebug -eq 1) {
		        sendMsg -a 1 -b 3013
                sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			    sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		    } else {
                sendMsg -a 1 -b 3013
            }
        }
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3028 }

        #
        # Transporta tabela Backup para Work Area
        if ($vDBSchm -ne $vWrkSchema) {
			if ($vDebug -eq 1) { sendMsg -a 1 -b 3029 }
			try {
                "alter table ${vDBSchm}.${vTabela}_bkp set schema ${vWrkSchema}" | Set-Content alter1.sql
                $vTranscriptFile = "${vCurrentdir}cmd.log"
                Start-Transcript -Path $vTranscriptFile
                cmd /c $cmdPsql -f alter1.sql -t
                Stop-Transcript
                $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
                if ($vChkErr) {
				    if ($vDebug -eq 1) { sendMsg -a 1 -b 3030 }
				    sendMsg -c "${vDBSchm}.${vTabela}_bkp, ${vWrkSchema}"
                    finalizaErro
                    Break
                }
			} catch {
                if ($vDebug -eq 1) {
		            sendMsg -a 1 -b 3013
                    sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			        sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		        } else {
                    sendMsg -a 1 -b 3013
                }
            }
			if ($vDebug -eq 1) { sendMsg -a 1 -b 3031 }
        }

        #
        # Renomear tabela nova para tabela original
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3032 }
		try {
            "alter table ${vWrkSchema}.${vTabela}_new rename to ${vTabelaAux}" | Set-Content rename2.sql
            $vTranscriptFile = "${vCurrentdir}cmd.log"
            Start-Transcript -Path $vTranscriptFile
            cmd /c $cmdPsql -f rename2.sql -t
            Stop-Transcript
            $vChkErr = Select-String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
            if ($vChkErr) {
                if ($vDebug -eq 1) { sendMsg -a 1 -b 3033 }
                finalizaErro
                Break
            }
        } catch {
                if ($vDebug -eq 1) {
		            sendMsg -a 1 -b 3013
                    sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			        sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		        } else {
                    sendMsg -a 1 -b 3013
                }
            }
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3034  }

        #
        # Transferir tabela nova para schema origem
        if ($vDBSchm -ne $vWrkSchema) {
		try {
            if ($vDebug -eq 1) { sendMsg -a 1 -b 3035  }
            "alter table ${vWrkSchema}.${vTabela}_new set schema ${vDBSchm}" | Set-Content alter2.sql
            $vTranscriptFile = "${vCurrentdir}cmd.log"
            Start-Transcript -Path $vTranscriptFile
            cmd /c $cmdPsql -f alter2.sql -t
            Stop-Transcript
            $vChkErr = Select -String cmd.log -Pattern ERROR -Quiet -SimpleMatch -CaseSensitive
            if ($vChkErr) {
                if ($vDebug -eq 1) { sendMsg -a 1 -b 3036  }
                finalizaErro
                Break
            }
		} catch {
                if ($vDebug -eq 1) {
		            sendMsg -a 1 -b 3013
                    sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
			        sendMsg -c "Exception Type: $($_.Exception.GetType().FullName)"
		        } else {
                    sendMsg -a 1 -b 3013
                }
        }
        if ($vDebug -eq 1) { sendMsg -a 1 -b 3037  }
        }

	#
    # Finaliza arquivos
    Copy-Item ddl.sql ddl.${vDHRFile}.${vTabela}.original.sql
    Copy-Item ddl.new.sql ddl.${VDHRFile}.${vTabela}.nova.sql
    limpaTemporarios
		
	} ## Fim Bloco Check de Qualidade

} ## Fim Bloco Mode 2

#
# Encerramento
finalizaSucesso



