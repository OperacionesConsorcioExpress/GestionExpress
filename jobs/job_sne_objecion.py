import logging

from model.gestion_sne_objecion import GestionSneObjecion


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


class TareasProgramadasSneObjecion:
    def sincronizar_objeciones_vencidas(self) -> dict:
        """
        Barrido diario de sne.gestion_sne para pasar a vencido los ICS
        cuyo debido proceso ya cerro y siguen en estado_objecion = 0.
        """
        logger.info("Iniciando sincronizacion diaria de objeciones vencidas SNE")
        with GestionSneObjecion() as gestion:
            resultado = gestion.sincronizar_objeciones_vencidas()

        logger.info(
            "Sincronizacion de objeciones vencidas finalizada. actualizados=%s lock=%s",
            resultado.get("actualizados", 0),
            resultado.get("lock_adquirido"),
        )
        return resultado


if __name__ == "__main__":
    TareasProgramadasSneObjecion().sincronizar_objeciones_vencidas()
