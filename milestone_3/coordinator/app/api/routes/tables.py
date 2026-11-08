from fastapi import APIRouter, Depends, HTTPException, status

from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.api.parameters import TableName
from coordinator.app.api.schemas.tables import TableDefinition
from coordinator.app.services.coordinator import Coordinator
from coordinator.app.storage.storage import TableAlreadyExistsError

router = APIRouter(prefix="/tables", tags=["Tables"])


@router.post("", response_model=TableDefinition, status_code=status.HTTP_201_CREATED)
def create_table(
    definition: TableDefinition,
    coordinator: Coordinator = Depends(get_coordinator),
):
    try:
        return coordinator.create_table(
            table_name=definition.table_name,
            partition_key=definition.partition_key,
            sort_key=definition.sort_key,
        )
    except TableAlreadyExistsError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error


@router.get("", response_model=list[TableDefinition])
def list_tables(
    coordinator: Coordinator = Depends(get_coordinator),
):
    return coordinator.list_tables()


@router.get("/{tableName}", response_model=TableDefinition)
def get_table(
    table_name: TableName,
    coordinator: Coordinator = Depends(get_coordinator),
):
    try:
        return coordinator.get_table(table_name)
    except KeyError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' does not exist",
        ) from error
