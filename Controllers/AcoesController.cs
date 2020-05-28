using System;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using APIAcoes.Models;

namespace APIAcoes.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AcoesController : ControllerBase
    {
        private static readonly Contador _CONTADOR = new Contador();
        private readonly ILogger<AcoesController> _logger;

        public AcoesController(ILogger<AcoesController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public object Get()
        {
            return new
            {
                NumeroMensagensEnviadas = _CONTADOR.ValorAtual
            };
        }

        [HttpPost]
        public object Post(
            [FromServices] IConfiguration config,
            Acao acao)
        {
            var conteudoAcao = JsonSerializer.Serialize(acao);
            _logger.LogInformation($"Dados: {conteudoAcao}");

            try
            {
                var body = Encoding.UTF8.GetBytes(conteudoAcao);
    	        string topic = config["Kafka_Topic"];
                var configKafka = new ProducerConfig
                {
                    BootstrapServers = config["Kafka_Broker"]
                };

                using (var producer = new ProducerBuilder<Null, string>(configKafka).Build())
                {
                    var result = producer.ProduceAsync(
                        topic,
                        new Message<Null, string>
                        { Value = conteudoAcao }).Result;

                    _logger.LogInformation(
                        $"Apache Kafka - Envio para o tópico {topic} do Apache Kafka concluído | " +
                        $"{conteudoAcao} | Status: { result.Status.ToString()}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }

            lock (_CONTADOR)
            {
                _CONTADOR.Incrementar();
            }

            return new
            {
                Resultado = "Mensagem enviada com sucesso!"
            };
        }
    }
}